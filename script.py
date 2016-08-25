import argparse
from collections import namedtuple
import itertools
import os
import pprint
import re
import sys
import traceback
from time import sleep

import botocore
import boto3
import paramiko
from paramiko.client import WarningPolicy
from retrying import retry
from sshtunnel import SSHTunnelForwarder


global debug_enabled
debug_enabled = False

SshEnvConfig = namedtuple('SshEnvConfig', [
    'username',
    'private_key_file_path',
    'remote_port',
    'use_bastion_tunnel'
])


def retry_if_throttled(exception):
    """Whether to retry a particular operation when an exception occurs.

    Currently only AWS throttling exceptions will trigger the retry
    mechanism.
    """
    return isinstance(exception, botocore.exceptions.ClientError) and \
        'throttling' in exception.message


class AwsManager(object):
    """ Handles interactions with AWS, and converts Boto responses into useful
    objects.
    """

    def __init__(self, do_dry_run=False):
        """
        Args:
            do_dry_run: if enabled, all operations are performed with a dry run
                        flag, with no side effects. See AWS/Boto3 docs for more
                        info.
        """
        self._do_dry_run = do_dry_run

    @retry(
        wait_exponential_multiplier=500,
        wait_exponential_max=10000,
        retry_on_exception=retry_if_throttled
    )
    def connect(self, autoscaling_client=None, ec2=None, ec2_client=None):
        """ Opens connections to AWS, specifically the autoscaling client and
            EC2 client and resource.

        Args:
            autoscaling_client: Override the autoscaling client.
            ec2: Override the EC2 resource.
            ec2_client: Override the EC2 client.
        """
        print('Connecting to AWS...')
        self._as_client = autoscaling_client or boto3.client('autoscaling')
        self._asg_paginator = self._as_client.get_paginator(
            'describe_auto_scaling_groups')
        self._ec2 = ec2 or boto3.resource('ec2')
        self._ec2_client = ec2_client or boto3.client('ec2')

    def get_all_as_groups(self):
        """ Retrieves all autoscaling groups accessible with the current
            credentials from AWS.

        Returns:
            a list of autoscaling groups
        """
        response = self._asg_paginator.paginate()
        return list(itertools.chain(
            *[asgs['AutoScalingGroups'] for asgs in response]
        ))

    def find_asg_group(self, asg_regex_pat):
        """ Searches for an autoscaling group using the specified regex.

        Args:
            asg_regex_pat: A Python regex string.
        Returns:
            a list of autoscaling groups where the AutoScalingGroupName matches
            the given regex.
        """
        all_asgs = self.get_all_as_groups()

        test_re = re.compile(asg_regex_pat)
        filtered_asgs = filter(lambda asg: test_re.match(
            asg['AutoScalingGroupName']), all_asgs)

        if len(filtered_asgs) == 0:
            return []
        elif len(filtered_asgs) > 1:
            return filtered_asgs
        else:
            return [filtered_asgs[0]]

    def get_expected_num_of_instances(self, asg):
        """ Gets the number of instances that we expect to be present in the
        autoscaling group.

        Args:
            asg: the autoscaling group info as given by find_asg_group() or
                 get_all_as_groups()
        Returns:
            the number of instances
        """

        return asg['DesiredCapacity']

    def terminate_instance(self, instance_id):
        """ Terminates the EC2 instance with the given instance ID.

        Args:
            instance_id: the Amazon instance ID to terminate
        """
        try:
            self._ec2_client.terminate_instances(
                DryRun=self._do_dry_run,
                InstanceIds=[
                    instance_id
                ]
            )
        except botocore.exceptions.ClientError as client_error:
            # Boto raises an exception to let you know
            # that the request would have succeeded
            # if the dry run flag was not in place.
            # Obviously.
            if 'DryRunOperation' not in client_error.response['Error']['Code']:
                raise client_error

    @retry(
        wait_exponential_multiplier=500,
        wait_exponential_max=10000,
        retry_on_exception=retry_if_throttled
    )
    def get_launch_config_for_asg(self, asg):
        """ Gets the launch configuration for the given autoscaling group.

        Args:
            asg: the autoscaling group info as given by find_asg_group() or
                 get_all_as_groups()
        Returns:
            a dict containing the launch configuration.
        """
        config_name = asg['LaunchConfigurationName']

        print('Retrieving autoscaling group launch configuration %s...' %
              config_name)

        launch_configs = self._as_client.describe_launch_configurations(
            LaunchConfigurationNames=[config_name])
        return launch_configs[u'LaunchConfigurations'][0]

    def get_instances_for_asg(self, asg):
        """ Gets all running instances belonging to an autoscaling group/

        Args:
            asg: the autoscaling group info as given by find_asg_group() or
                 get_all_as_groups()
        Returns:
            a list of instances
        """
        asg_name = asg['AutoScalingGroupName']
        instances = self._ec2.instances.filter(
            Filters=[
                {'Name': 'instance-state-name', 'Values': ['running']},
                {'Name': 'tag:aws:autoscaling:groupName', 'Values': [asg_name]}
            ]
        )
        return list(instances)

    def get_volumes_dict_for_instance(self, instance):
        """ Gets EBS volume information for an instance.

        Args:
            instance: the EC2 instance, as given by, e.g.
            get_instances_for_asg()
        Returns:
            a dictionary with the device names as keys, and the corresponding
            volume information as items, e.g. { "deviceName": { ... } }
        """
        volume_ids = [ebs['Ebs']['VolumeId']
                      for ebs in instance.block_device_mappings]
        volumes_response = \
            self._ec2_client.describe_volumes(VolumeIds=volume_ids)['Volumes']

        return {
            current_volume['Attachments'][0]['Device']: current_volume
            for current_volume in volumes_response
        }

    def config_volumes_to_dict(self, block_device_mapping_config):
        """ Converts the block device mapping given in an autoscaling group
        launch configuration to a dictionary, e.g. { "deviceName": { ... } }

        Args:
            block_device_mapping_config: The BlockDeviceMappings portion of the
           7 autoscaling group launch configuration (as given by
           get_launch_config_for_asg())
        Returns:
            a dictionary with the device names as keys, and the corresponding
            volume information as items, e.g. { "deviceName": { ... } }
        """
        return {
            current_volume['DeviceName']: current_volume
            for current_volume in block_device_mapping_config
        }

    def get_userdata_for_instance(self, instance_id):
        """ Gets the Base64-encoded AWS Userdata for a particular instance.

        Args:
            instance_id: Amazon EC2 instance ID.
        Returns:
            the userdata of the instance.
        """
        response = self._ec2_client.describe_instance_attribute(
            InstanceId=instance_id, Attribute='userData')
        return response['UserData']['Value']


class InstanceSshManager(object):
    """ A simple interface for SSHing into an instance.

    We need to check whether an instance has successfully booted, via SSHing
    into the instance.
    """

    @staticmethod
    def get_instance(ssh_config, ssh_client=None):
        """ Factory method to get right implementation of InstanceSshManager
            depending on SSH config.

            E.g. one subclass performs some extra steps to set up an SSH tunnel

        Args:
            ssh_config: SshEnvConfig tuple containing SSH parameters
            ssh_client: Use to override the default Paramiko SSHClient
        """
        if ssh_config.use_bastion_tunnel:
            return InstanceSshManagerWithSshTunnel(ssh_config, ssh_client)
        else:
            return InstanceSshManager(ssh_config, ssh_client)

    def __init__(self, ssh_config, ssh_client=None):
        """Inits a default InstanceSshManager without SSH tunnelling.

        Args:
            ssh_config: SshEnvConfig tuple containing SSH parameters
            ssh_client: Use to override the default Paramiko SSHClient
        """
        self._sshclient = ssh_client or paramiko.SSHClient()
        self._ssh_config = ssh_config
        self._connected = False

    def connect(self, ip_address):
        """ Opens an SSH connection to an instance.

        Args:
            ip_address: IPv4 address of the instance to SSH into.
        Raises:
            IOError: if this class already has a connection.
        """
        if self._connected:
            raise IOError("Already connected")
        self._create_connection(ip_address)
        self._connected = True

    def _create_connection(self, ip_address):
        self._sshclient.set_missing_host_key_policy(WarningPolicy())
        self._sshclient.connect(
            ip_address,
            username=self._ssh_config.username,
            key_filename=self._ssh_config.private_key_file_path
        )

    def is_ready(self, ip_address):
        """ Returns whether an instance has successfully booted or not yet.

        The ability to SSH into an instance by itself does not indicate whether
        it has booted and is ready; EC2 creates a file at
        /var/lib/cloud/instance/boot-finished when an instance has finished
        booting. This method simply connects via SSH and checks for that file.

        Args:
            ip_address: IPv4 address of the instance to SSH into.
        Returns:
            boolean indicating whether the instance has booted or not.
        """
        try:
            self.connect(ip_address)

            stdin, stdout, stderr = self._sshclient.exec_command(
                "ls /var/lib/cloud/instance/boot-finished"
            )
            exit_code = stdout.channel.recv_exit_status()

            debug('Received exit code %d from IP %s' % (exit_code, ip_address))
            return exit_code == 0
        except:
            debug('Exception raised whilst SSHing into IP %s' % (ip_address))
            traceback.print_exc()
            return False
        finally:
            self.close_connections()

    def close_connections(self):
        """ Closes the current SSH connection."""
        self._sshclient.close()
        self._connected = False


class InstanceSshManagerWithSshTunnel(InstanceSshManager):
    """ Wraps the SSH connection in an SSH tunnel.

    When connecting to instances locally we connect through a Bastion host
    and need to proxy through using an SSH tunnel.
    """

    def _create_connection(self, ip_address):
        self._ssh_tunnel = self._get_ssh_tunnel(
            self._ssh_config,
            ip_address
        )

        self._sshclient.set_missing_host_key_policy(WarningPolicy())
        self._sshclient.connect(
            "localhost",
            port=self._ssh_tunnel.local_bind_port,
            username=self._ssh_config.username,
            key_filename=self._ssh_config.private_key_file_path
        )

    def close_connections(self):
        super(InstanceSshManagerWithSshTunnel, self).close_connections()

        self._ssh_tunnel.close()
        self._ssh_tunnel = None

    def _get_ssh_tunnel(self, ssh_config, host_ip_address):
        ssh_tunnel = SSHTunnelForwarder(
            (args.ssh_tunnel,22),
            ssh_username=ssh_config.username,
            ssh_private_key=ssh_config.private_key_file_path,
            remote_bind_address=(host_ip_address, ssh_config.remote_port),
            set_keepalive=30
        )
        ssh_tunnel.start()
        while not ssh_tunnel.tunnel_is_up:
            ssh_tunnel.check_local_side_of_tunnels()
            sleep(1)
        return ssh_tunnel


class InstanceConfigComparator(object):
    """ Contains methods for comparing an instance to the launch configuration.
    """

    def compare_to_config(
        self,
        instance,
        asg_launch_config,
        instance_userdata
    ):
        """ Compares an instance config to the autoscaling group launch
            configuration.

        Does not include any differences between the EBS volumes.

        Compares ImageId, InstanceType, KernelId, KeyName, which are required -
        an error is raised if these are missing from either the instance or the
        launch configuration.

        Also compares IamInstanceProfile, but this can be missing from both the
        instance and launch config. If it is only present in one of them, a
        difference will be returned.

        Args:
            instance: the AWS EC2 instance
            asg_launch_config: the autoscaling launch configuration
            instance_userdata: the EC2 instance userdata (which sadly does not
                               come with the rest of the user data)
        Returns:
            A list of differences between the instance config and launch config
            keyed by their launch configuration name, e.g.
            ['InstanceType', 'KeyName', 'IamInstanceProfile']
        Raises:
            AttributeError if the required attributes are missing from the
            instance configuration or launch configuration
        """
        change_list = []

        if instance_userdata != asg_launch_config['UserData']:
            change_list.append('UserData')

        instance_sg = sorted([sg['GroupId']
                              for sg in instance.security_groups])
        config_sg = sorted(asg_launch_config['SecurityGroups'])

        if instance_sg != config_sg:
            change_list.append('SecurityGroups')

        def check_attr(instance_attr_name, config_attr_name):
            try:
                instance_attr = getattr(instance, instance_attr_name) or ''
                config_attr = asg_launch_config[config_attr_name] or ''

                if config_attr != '' and instance_attr != config_attr:
                    change_list.append(config_attr_name)
            except KeyError:
                raise AttributeError('Launch configuration response was '
                                     'missing required attribute ' +
                                     config_attr_name)

        check_attr('image_id', 'ImageId')
        check_attr('instance_type', 'InstanceType')
        check_attr('kernel_id', 'KernelId')
        check_attr('key_name', 'KeyName')

        instance_iam_profile = getattr(
            instance, 'iam_instance_profile', '') or ''
        config_iam_profile = asg_launch_config.get(
            'IamInstanceProfile', '') or ''

        if instance_iam_profile != config_iam_profile:
            change_list.append('IamInstanceProfile')

        return change_list

    def compare_volumes_config(
        self,
        instance_volumes_dict,
        config_volumes_dict
    ):
        """ Compares EBS volume configuration between the instance and launch
        config.

        If the launch configuration contains no volume mappings, the instance
        should have a default EBS configuration so if it has one mapping and
        the config has none, this function will return no difference.

        If there are differences in device names, e.g. devices have been added
        or removed, this script will return 'DeviceName:sdaX' for each device
        that is different.

        Otherwise, the VolumeType, VolumeSize and DeleteOnTermination flags
        will be compared and any differences will be returned.

        Args:
            instance_volumes_dict: Instance configuration with device names as
                                   keys and config as values.
            config_volumes_dict: Config configuration with device names as
                                 keys and config as values.
        Returns:
            A list of differences between the volume configurations.
        """
        if not len(config_volumes_dict) and len(instance_volumes_dict) == 1:
            return []

        device_name_differences = set(instance_volumes_dict.keys()) ^  \
            set(config_volumes_dict.keys())

        if len(device_name_differences):
            return ['DeviceName:%s' % dev for dev in
                    sorted(device_name_differences)]

        change_list = []

        for device_name, instance_volume in instance_volumes_dict.iteritems():

            config_volume = config_volumes_dict[device_name]

            if (instance_volume['VolumeType'] !=
                    config_volume['Ebs']['VolumeType']):
                change_list.append(
                    device_name + '.BlockDeviceMappings.Ebs.VolumeType')
            if instance_volume['Size'] != config_volume['Ebs']['VolumeSize']:
                change_list.append(
                    device_name + '.BlockDeviceMappings.Ebs.Size')
            if (instance_volume['Attachments'][0]['DeleteOnTermination'] !=
                    config_volume['Ebs']['DeleteOnTermination']):
                change_list.append(device_name + '.BlockDeviceMappings' +
                                   '.Ebs.DeleteOnTermination')

        return change_list


class RollingUpgradeManager(object):
    """ Manages the whole rolling upgrade process.
    """

    @staticmethod
    def get_oldest_instance(instances):
        """ Given a list of instances, gets the instance that was launched
        first.

        Args:
            instances: List of EC2 instances.
        """
        return sorted(instances, key=lambda instance: instance.launch_time)[0]

    def __init__(
        self,
        ssh_config,
        do_dry_run=False,
        max_wait_attempts=40,
        sleep_time_s=30,
        aws_manager=None,
        instance_manager=None,
        instance_config_comparator=None
    ):
        """
        Args:
            ssh_config: SshEnvConfig tuple containing SSH parameters
            do_dry_run: If enabled, will perform the operation without any side
                        effects (will not terminate any instances)
            max_wait_attempts: Number of attempts to wait for instances to boot
                               up before failing.
            sleep_time_s: Time in seconds to sleep before waiting for instances
                          to boot up
            aws_manager: Use to override the AwsManager instance.
            instance_manager: Use to override the InstanceManager instance.
            instance_config_comparator: Use to override the
                                        InstanceConfigComparator.
        """
        self._sleep_time_s = sleep_time_s
        self._max_wait_attempts = max_wait_attempts
        self._aws_manager = aws_manager or AwsManager(do_dry_run)
        self._instance_manager = (instance_manager or
                                  InstanceSshManager.get_instance(ssh_config))
        self._instance_config_comparator = (instance_config_comparator or
                                            InstanceConfigComparator())

    def connect(self, autoscaling_client=None, ec2=None, ec2_client=None):
        """ Connects to AWS. """
        self._aws_manager.connect(autoscaling_client, ec2, ec2_client)

    def _get_single_asg(self,asg_slug):
        regex_pat = '^%s' % (asg_slug)
        as_group_list = self._aws_manager.find_asg_group(regex_pat)
        if len(as_group_list) != 1:
            raise Exception(
                'Found %d autoscaling groups with regex "%s", expected 1' % (
                    len(as_group_list), regex_pat
                ))

        return as_group_list[0]

    def _on_still_waiting_for_boot(self, current_attempts, expected_num_instances, instances):
        print(('%d instances have booted' % len(instances)) +
              (" - Waiting for %d more..." %
               (expected_num_instances - len(instances))
               ))
        debug('Instances: ' + pprint.pformat(instances))
        print('Attempt %d of %d' % (current_attempts,
                                    self._max_wait_attempts))

    def wait_for_instances(self, asg, expected_num_instances):
        """ Waits for the expected number of instances to be available and
            booted.

        Args:
            asg: autoscaling group to find instances in
            expected_num_instances: how many instances to wait for. Typically
                                    should be the 'DesiredSize' of the ASG
        """
        current_attempts = 0

        instances = self._aws_manager.get_instances_for_asg(asg)

        while (current_attempts < self._max_wait_attempts):

            if len(instances) >= expected_num_instances:
                print('=== All instances have booted ===')

                if self.are_all_instances_ready(instances):
                    print('=== All instances have completed cloud-init ===')
                    break
                else:
                    print('Waiting for instances to finish cloud-init, '
                          'attempt %d of %d' % (current_attempts,
                                                self._max_wait_attempts))
            else:
                self._on_still_waiting_for_boot(current_attempts,
                                                expected_num_instances,
                                                instances
                                                )

            instances = self._aws_manager.get_instances_for_asg(asg)
            self.wait()
            current_attempts += 1

            if current_attempts >= self._max_wait_attempts:
                print("Tried " + str(current_attempts) +
                      " with no success - Exiting.")
                sys.exit(1)

    def wait(self):
        sleep(self._sleep_time_s)

    def are_all_instances_ready(self, instances):
        """ Returns whether the instances have booted and are ready.

        See InstanceManager.is_ready() for further details.

        Args:
            instances: list of EC2 instances to check
        Returns:
            True if all instances have booted, False if at least one hasn't.
        """
        for instance in instances:
            if not self._instance_manager.is_ready(instance.private_ip_address):
                return False
        return True

    def compare_instance_to_config(self, instance, config):
        """ Compares a single instance to the launch configuration.

        Mostly gets the configuration from AWS and passes it to the
        InstanceConfigComparator.
        Returns:
            a list of differences between the instance and the configuration.
        """
        instance_userdata = self._aws_manager.get_userdata_for_instance(
            instance.id)

        instance_changes = self._instance_config_comparator.compare_to_config(
            instance, config, instance_userdata)

        instance_volume_dict = self._aws_manager.get_volumes_dict_for_instance(
            instance)

        config_volumes_dict = self._aws_manager.config_volumes_to_dict(
            config['BlockDeviceMappings'])

        volume_changes = self._instance_config_comparator.compare_volumes_config(
            instance_volume_dict, config_volumes_dict)

        return instance_changes + volume_changes

    def get_instances_to_upgrade(self, asg, config):
        """ Gets a list of the instances that need upgrading.

        Gets all the instances for the given ASG and checks them against the
        configuration.

        Args:
            asg: the autoscaling group to get instances from
            config: the launch configuration to check
        Returns:
            a list of instances that differ from the launch configuration.
        """
        asg_instances = self._aws_manager.get_instances_for_asg(asg)
        instances_to_upgrade = set()
        for instance in asg_instances:
            diffs = self.compare_instance_to_config(instance, config)

            if len(diffs):
                debug('=== Found differences between instance %s and config:\n%s' %
                      (instance.id, diffs))
                instances_to_upgrade.add(instance)
        return list(instances_to_upgrade)

    def perform_rolling_upgrade_where_needed(self, asg_slug):
        """ Upgrades instances in an autoscaling group if they are different
            from the launch configuration.

        Args:
            asg_slug: Name of autoscaling group to upgrade, e.g. "RabbitMq"
        """
        asg = self._get_single_asg(asg_slug)

        print('Found matching AutoScalingGroup called %s' %
              asg['AutoScalingGroupName'])
        debug('AutoScalingGroup: %s\n' % pprint.pformat(asg))

        config = self._aws_manager.get_launch_config_for_asg(asg)

        expected_num_instances = self._aws_manager.get_expected_num_of_instances(
            asg)

        while True:
            self.wait_for_instances(asg, expected_num_instances)

            instances_to_upgrade = self.get_instances_to_upgrade(asg, config)

            if not len(instances_to_upgrade):
                print('=== No differences between instances and configuration'
                      ' found, exiting ===')
                break
            else:
                print(str(len(instances_to_upgrade)) + ' instance(s) that do'
                      ' not match the configuration')

            instance = RollingUpgradeManager.get_oldest_instance(
                instances_to_upgrade)

            print "!!! Going to kill " + instance.id

            self._aws_manager.terminate_instance(instance.id)


def parse_args():

    parser = argparse.ArgumentParser('')
    parser.add_argument(
        '-l', '--limit',
        help='limit to these hosts only',
        default='')
    parser.add_argument(
        '--ssh_tunnel',
        help='the address of a bastion host to tunnel through'
    )
    parser.add_argument(
        '--ssh_private_key',
        help='The ssh key to be used',
    )
    parser.add_argument(
        '--ssh_username',
        help='The ssh username to be used',
        default='centos'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Do some debugging'
    )
    parser.add_argument(
        '--dry_run',
        action='store_true',
        help='Stop any actions being performed on the AWS account'
    )
    parser.add_argument(
        '--max_wait_attempts',
        help='The maximum number of attemts to wait for an instance before stopping',
        default=40
    )
    parser.add_argument(
        '--sleep',
        help='The number of seconds to wait between attempts of checking the instances',
        default=30
    )

    return parser.parse_args()


def debug(msg):
    if debug_enabled:
        print(msg)

if __name__ == '__main__':
    args = parse_args()

    debug_enabled = args.debug

    ssh_config = SshEnvConfig(
        username=args.ssh_username,
        private_key_file_path=args.ssh_private_key,
        remote_port=22,
        use_bastion_tunnel=args.ssh_tunnel
    )

    rum = RollingUpgradeManager(
        ssh_config=ssh_config,
        max_wait_attempts=int(args.max_wait_attempts),
        sleep_time_s=int(args.sleep),
        do_dry_run=args.dry_run
    )

    print('Starting rolling upgrade for host %s' % (
        args.limit))
    debug('Arguments passed:\n%s' % pprint.pformat(args))

    rum.connect()
    rum.perform_rolling_upgrade_where_needed(args.limit)
