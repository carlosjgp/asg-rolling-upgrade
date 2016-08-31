from collections import namedtuple
from datetime import datetime
from mock import mock

import pytest
import botocore
from paramiko import SSHClient

from asg_rolling_upgrade import (
    RollingUpgradeManager,
    AwsManager,
    retry_if_throttled,
    InstanceSshManager,
    InstanceConfigComparator,
    SshEnvConfig
)


class MockObject:
    def __init__(self, **kw):
        for name in kw:
            setattr(self, name, kw[name])


@pytest.fixture()
def mock_paginator():
    return mock.Mock(spec=botocore.client.Paginator)


@pytest.fixture()
def mock_as_client(mock_paginator):
    mock_as_client = mock.Mock()
    mock_as_client.get_paginator.return_value = mock_paginator
    return mock_as_client


@pytest.fixture()
def mock_ec2():
    return mock.Mock(spec=botocore.client)


@pytest.fixture()
def mock_ec2_client():
    return mock.Mock()


@pytest.fixture()
def aws_manager(mock_as_client, mock_ec2, mock_ec2_client):
    aws_manager = AwsManager()
    aws_manager.connect(autoscaling_client=mock_as_client,
                        ec2=mock_ec2, ec2_client=mock_ec2_client)
    return aws_manager


def test_only_retries_on_throttle():
    ex = botocore.exceptions.ClientError(
        {'Error': {'Message': 'throttling'}}, 'test_connect')
    assert retry_if_throttled(ex)

    ex = botocore.exceptions.ClientError(
        {'Error': {'Message': 'different error'}}, 'test_connect')
    assert not retry_if_throttled(ex)


def test_aws_finds_asg_group_no_match(aws_manager, mock_paginator):
    mock_paginator.paginate.return_value = [{
        'AutoScalingGroups': [
            {'AutoScalingGroupName': 'foo'},
            {'AutoScalingGroupName': 'bar'},
            {'AutoScalingGroupName': 'lll'},
            {'AutoScalingGroupName': 'ooo'},
            {'AutoScalingGroupName': 'mmm'}
        ]
    }]

    result = aws_manager.find_asg_group('^no_match')

    assert result == []


def test_aws_finds_multiple_asg_groups(aws_manager, mock_paginator):
    mock_paginator.paginate.return_value = [{
        'AutoScalingGroups': [
            {'AutoScalingGroupName': 'foo'},
            {'AutoScalingGroupName': 'match1'},
            {'AutoScalingGroupName': 'match2'},
            {'AutoScalingGroupName': 'match3'},
            {'AutoScalingGroupName': 'test_pat_1'}
        ]
    }]

    results = aws_manager.find_asg_group('^match')

    assert len(results) == 3
    assert results[0]['AutoScalingGroupName'] == 'match1'
    assert results[1]['AutoScalingGroupName'] == 'match2'
    assert results[2]['AutoScalingGroupName'] == 'match3'


def test_aws_finds_asg_group(aws_manager, mock_paginator):
    mock_paginator.paginate.return_value = [{
        'AutoScalingGroups': [
            {'AutoScalingGroupName': 'foo'},
            {'AutoScalingGroupName': 'bar'},
            {'AutoScalingGroupName': 'lll'},
            {'AutoScalingGroupName': 'ooo'},
            {'AutoScalingGroupName': 'test_pat_1'}
        ]
    }]

    result = aws_manager.find_asg_group('^test_pat')

    assert len(result) == 1
    assert result[0]['AutoScalingGroupName'] == 'test_pat_1'


def test_aws_terminate_instance_fails_on_client_error_not_dry_run(
    aws_manager,
    mock_ec2_client
):
    err_response = {'Error': {'Code': 'TestError'}}
    mock_ec2_client.terminate_instances.side_effect = \
        botocore.exceptions.ClientError(err_response, 'Test')

    with pytest.raises(botocore.exceptions.ClientError):
        aws_manager.terminate_instance('test_instance_id')


def test_aws_terminate_instance_succeeds_if_dry_run(
    aws_manager,
    mock_ec2_client
):
    err_response = {'Error': {'Code': 'DryRunOperation'}}
    mock_ec2_client.terminate_instances.side_effect = \
        botocore.exceptions.ClientError(err_response, 'Test')

    aws_manager.terminate_instance('test_instance_id')
    # should not reraise the exception


def test_aws_converts_volumes_to_dict(aws_manager, mock_ec2_client):
    class Instance:
        block_device_mappings = []
    instance = Instance()

    volume_ids = ['test_id_1', 'test_id_2', 'test_id_3']

    instance.block_device_mappings = \
        [{'Ebs': {'VolumeId': id}} for id in volume_ids]

    dev_ids = ['sda1', 'sda2', 'sda3', 'sda4']

    ec2_volumes = [{'Attachments': [{'Device': id}]} for id in dev_ids]

    mock_ec2_client.describe_volumes.return_value = {'Volumes': ec2_volumes}

    result = aws_manager.get_volumes_dict_for_instance(instance)
    mock_ec2_client.describe_volumes.assert_called_with(VolumeIds=volume_ids)

    for i, key in enumerate(dev_ids):
        assert key in result
        assert result[key] == ec2_volumes[i]


def test_aws_config_volume_to_dict(aws_manager):
    devs = ('sda1', 'sda2', 'sda3', 'sda4')

    block_device_mapping_config = [{'DeviceName': dev_id} for dev_id in devs]

    result = aws_manager.config_volumes_to_dict(block_device_mapping_config)

    for i, id in enumerate(devs):
        assert id in result
        assert result[id] == block_device_mapping_config[i]


@pytest.fixture
def mock_ssh_connection():
    return mock.Mock(spec=SSHClient)


@pytest.fixture
def instance_manager(mock_ssh_connection):
    ssh_config = SshEnvConfig(
        username='test_user',
        private_key_file_path='/path/to/key',
        environment='TestEnv',
        remote_port=22,
        use_bastion_tunnel=False
    )

    return InstanceSshManager(ssh_config, ssh_client=mock_ssh_connection)

Instance = namedtuple('Instance', ['private_ip_address'])


def test_instance_manager_error_if_connected_twice(instance_manager):
    instance_manager.connect(Instance('10.0.0.0'))

    with pytest.raises(Exception):
        instance_manager.connect(Instance('10.0.0.10'))


def test_instance_manager_is_ready_returns_false_on_ssh_error(
    instance_manager,
    mock_ssh_connection
):
    mock_ssh_connection.exec_command.side_effect = Exception('SSH error')

    assert not instance_manager.is_ready(Instance('10.0.0.0'))

    mock_ssh_connection.close.assert_called_with()


def test_instance_manager_returns_false_on_non_zero_exit_code(
    instance_manager,
    mock_ssh_connection
):
    mock_ssh_stdout = mock.Mock()

    mock_ssh_stdout.channel = mock.Mock()
    mock_ssh_stdout.channel.recv_exit_status.return_value = 1

    mock_ssh_connection.exec_command.return_value = (
        None, mock_ssh_stdout, None,)

    assert not instance_manager.is_ready(Instance('10.0.0.0'))


@pytest.fixture(scope='function')
def instance_comparator(request):
    return InstanceConfigComparator()


CompareConfigTestParams = namedtuple('CompareConfigTestParams',
                                     ['instance_attr_name', 'asg_key_name',
                                      'instance_val', 'asg_val'])


class InstanceForConfigComparator(object):
    security_groups = []
    image_id = ''
    instance_type = ''
    kernel_id = ''
    key_name = ''


@pytest.fixture(scope='function')
def default_asg_config(request):
    return {
        'UserData': 'userdata',
        'SecurityGroups': [],
        'ImageId': '',
        'InstanceType': '',
        'KernelId': '',
        'KeyName': ''
    }


@pytest.mark.parametrize('test_params', [
    CompareConfigTestParams('image_id', 'ImageId', 'currentId', 'newId'),
    CompareConfigTestParams('instance_type', 'InstanceType', 't2.micro',
                            't2.medium'),
    CompareConfigTestParams('kernel_id', 'KernelId', 'ubuntu-1',
                            'debian-2'),
    CompareConfigTestParams('key_name', 'KeyName', 'crunch-dev',
                            'crunch-prod'),
    CompareConfigTestParams('iam_instance_profile', 'IamInstanceProfile',
                            'go-agent', 'user'),
    CompareConfigTestParams('iam_instance_profile', 'IamInstanceProfile',
                            'go-agent', ''),
    CompareConfigTestParams('iam_instance_profile', 'IamInstanceProfile',
                            'go-agent', None),
    CompareConfigTestParams('iam_instance_profile', 'IamInstanceProfile',
                            '', 'user'),
    CompareConfigTestParams('iam_instance_profile', 'IamInstanceProfile',
                            None, 'user'),
])
def test_comparator_compare_config_differences(
    instance_comparator,
    default_asg_config,
    test_params
):
    instance = InstanceForConfigComparator()
    setattr(instance, test_params.instance_attr_name, test_params.instance_val)
    default_asg_config[test_params.asg_key_name] = test_params.asg_val

    result = instance_comparator.compare_to_config(
        instance, default_asg_config, 'userdata')

    assert len(result) == 1
    assert result[0] == test_params.asg_key_name


@pytest.mark.parametrize('test_params', [
    CompareConfigTestParams('image_id', 'ImageId', 'currentId', ''),
    CompareConfigTestParams('instance_type', 'InstanceType', 't2.micro', ''),
    CompareConfigTestParams('kernel_id', 'KernelId', 'ubuntu-1', ''),
    CompareConfigTestParams('key_name', 'KeyName', 'crunch-dev', ''),
])
def test_comparator_compare_config_error_on_missing_config_attr(
    instance_comparator,
    default_asg_config,
    test_params
):
    instance = InstanceForConfigComparator()
    setattr(instance, test_params.instance_attr_name, test_params.instance_val)

    del default_asg_config[test_params.asg_key_name]

    with pytest.raises(AttributeError):
        instance_comparator.compare_to_config(
            instance, default_asg_config, 'userdata')


@pytest.mark.parametrize('test_params', [
    CompareConfigTestParams('image_id', 'ImageId', 'currentId', ''),
    CompareConfigTestParams('instance_type', 'InstanceType', 't2.micro', ''),
    CompareConfigTestParams('kernel_id', 'KernelId', 'ubuntu-1', ''),
    CompareConfigTestParams('key_name', 'KeyName', 'crunch-dev', ''),
])
def test_comparator_compare_config_error_on_missing_instance_attr(
    instance_comparator,
    default_asg_config,
    test_params
):
    class Instance(object):
        security_groups = []
    instance = Instance()
    default_asg_config[test_params.asg_key_name] = test_params.instance_val

    with pytest.raises(AttributeError):
        instance_comparator.compare_to_config(
            instance, default_asg_config, 'userdata')


@pytest.mark.parametrize('test_params', [
    CompareConfigTestParams('image_id', 'ImageId', 'currentId', ''),
    CompareConfigTestParams('instance_type', 'InstanceType', 't2.micro', ''),
    CompareConfigTestParams('kernel_id', 'KernelId', 'ubuntu-1', ''),
    CompareConfigTestParams('key_name', 'KeyName', 'crunch-dev', ''),
    CompareConfigTestParams('iam_instance_profile', 'IamInstanceProfile',
                            'go-agent', ''),
])
def test_comparator_compare_config_same(
    instance_comparator,
    default_asg_config,
    test_params
):
    instance = InstanceForConfigComparator()
    setattr(instance, test_params.instance_attr_name, test_params.instance_val)
    default_asg_config[test_params.asg_key_name] = test_params.instance_val

    result = instance_comparator.compare_to_config(
        instance, default_asg_config, 'userdata')

    assert len(result) == 0


def test_comparator_userdata_difference(instance_comparator,
                                        default_asg_config):
    instance = InstanceForConfigComparator()

    default_asg_config['UserData'] = 'new_userdata'

    result = instance_comparator.compare_to_config(
        instance, default_asg_config, 'old_userdata')

    assert len(result) == 1
    assert result[0] == 'UserData'


def test_comparator_security_groups_difference(instance_comparator,
                                               default_asg_config):
    instance = InstanceForConfigComparator()
    instance.security_groups = [
        {'GroupId': 'sg-old-1'}, {'GroupId': 'sg-old-2'}]

    default_asg_config['SecurityGroups'] = ['sg-new-1', 'sg-new-2']

    result = instance_comparator.compare_to_config(
        instance, default_asg_config, 'userdata')

    assert len(result) == 1
    assert result[0] == 'SecurityGroups'


def test_comparator_security_groups_order_difference(instance_comparator,
                                                     default_asg_config):
    instance = InstanceForConfigComparator()
    instance.security_groups = [{'GroupId': 'sg-1'}, {'GroupId': 'sg-2'}]

    default_asg_config['SecurityGroups'] = ['sg-2', 'sg-1']

    result = instance_comparator.compare_to_config(
        instance, default_asg_config, 'userdata')

    assert len(result) == 0

ComparatorVolumeDeviceNamesParams = namedtuple(
    'ComparatorVolumeDeviceNamesParams',
    ['instance_device_keys', 'config_device_keys', 'expected_result']
)


@pytest.fixture(scope='function')
def default_instance_volume_dict(request):
    return {
        'VolumeType': '',
        'Size': '8',
        'Attachments': [{'Device': 'test_device_name',
                         'DeleteOnTermination': 'true'}]
    }


@pytest.fixture(scope='function')
def default_config_volume_dict(request):
    return {
        'Ebs': {
            'VolumeType': '',
            'VolumeSize': '8',
            'DeleteOnTermination': 'true'
        },
        'DeviceName': 'test_device_name'
    }


@pytest.mark.parametrize('test_params', [
    ComparatorVolumeDeviceNamesParams(['sda1'], ['sda2'],
                                      ['DeviceName:sda1', 'DeviceName:sda2']),
    ComparatorVolumeDeviceNamesParams(['sda1'], ['sda1', 'sda2'],
                                      ['DeviceName:sda2']),
    ComparatorVolumeDeviceNamesParams(['sda2', 'sda1'], ['sda1'],
                                      ['DeviceName:sda2']),

    # config has no volumes defined, but instance has more than one defined
    ComparatorVolumeDeviceNamesParams(['sda1', 'sda2'], [],
                                      ['DeviceName:sda1', 'DeviceName:sda2']),

    # device names should be same
    ComparatorVolumeDeviceNamesParams(['sda1'], ['sda1'], []),
    ComparatorVolumeDeviceNamesParams(['sda2'], ['sda2'], []),
    ComparatorVolumeDeviceNamesParams(['sda1', 'sda2'], ['sda2', 'sda1'], []),
])
def test_comparator_volume_device_names(
    instance_comparator,
    test_params,
    default_instance_volume_dict,
    default_config_volume_dict
):
    instance_volumes_dict = {key: default_instance_volume_dict
                             for key in test_params.instance_device_keys}
    config_volumes_dict = {key: default_config_volume_dict
                           for key in test_params.config_device_keys}

    result = instance_comparator.compare_volumes_config(
        instance_volumes_dict, config_volumes_dict)

    assert result == test_params.expected_result


def test_if_config_has_no_volumes_instance_only_has_one_volume(
    instance_comparator,
    default_instance_volume_dict,
    default_config_volume_dict
):
    instance_volumes_dict = {'sda1': default_instance_volume_dict}
    config_volumes_dict = {}

    result = instance_comparator.compare_volumes_config(
        instance_volumes_dict, config_volumes_dict)

    assert result == []


VolumeAttributesTestParams = namedtuple('VolumeAttributesTestParams',
                                        ['instance_volumes_dict',
                                         'config_volumes_dict',
                                         'expected_result']
                                        )


@pytest.mark.parametrize('test_params', [
    VolumeAttributesTestParams(
        {'sda1': {'VolumeType': 'gp2',
                  'Size': '8',
                  'Attachments': [{'Device': 'test_device_name',
                                   'DeleteOnTermination': 'true'}]}},
        {'sda1': {'Ebs': {
            'VolumeType': 'standard',
            'VolumeSize': '8',
            'DeleteOnTermination': 'true'
        }, 'DeviceName': 'test_device_name'}},
        ['sda1.BlockDeviceMappings.Ebs.VolumeType']),
    VolumeAttributesTestParams(
        {'sda1': {'VolumeType': 'gp2',
                  'Size': '16',
                  'Attachments': [{'Device': 'test_device_name',
                                   'DeleteOnTermination': 'true'}]}},
        {'sda1': {'Ebs': {
            'VolumeType': 'gp2',
            'VolumeSize': '8',
            'DeleteOnTermination': 'true'
        }, 'DeviceName': 'test_device_name'}},
        ['sda1.BlockDeviceMappings.Ebs.Size']),
    VolumeAttributesTestParams(
        {'sda1': {'VolumeType': 'gp2',
                  'Size': '8',
                  'Attachments': [{'Device': 'test_device_name',
                                   'DeleteOnTermination': 'false'}]}},
        {'sda1': {'Ebs': {
            'VolumeType': 'gp2',
            'VolumeSize': '8',
            'DeleteOnTermination': 'true'
        }, 'DeviceName': 'test_device_name'}},
        ['sda1.BlockDeviceMappings.Ebs.DeleteOnTermination']),
    VolumeAttributesTestParams(
        {'sda1': {'VolumeType': 'gp2',
                  'Size': '8',
                  'Attachments': [{'Device': 'test_device_name',
                                   'DeleteOnTermination': 'true'}]},
         'sda2': {'VolumeType': 'gp2',
                  'Size': '8',
                  'Attachments': [{'Device': 'test_device_name',
                                   'DeleteOnTermination': 'true'}]}},
        {'sda1': {'Ebs': {
            'VolumeType': 'gp2',
            'VolumeSize': '8',
            'DeleteOnTermination': 'true'
        }, 'DeviceName': 'test_device_name'},
            'sda2': {'Ebs': {
                'VolumeType': 'standard',
                'VolumeSize': '16',
                'DeleteOnTermination': 'false'
            }, 'DeviceName': 'test_device_name'}},
        ['sda2.BlockDeviceMappings.Ebs.VolumeType',
         'sda2.BlockDeviceMappings.Ebs.Size',
         'sda2.BlockDeviceMappings.Ebs.DeleteOnTermination']),
])
def test_volumes_attributes_differences(
    test_params,
    instance_comparator,
    default_instance_volume_dict,
    default_config_volume_dict
):
    result = instance_comparator.compare_volumes_config(
        test_params.instance_volumes_dict, test_params.config_volumes_dict)
    print(result)
    assert result == test_params.expected_result


@pytest.fixture()
def mock_aws_manager():
    return mock.Mock(spec=AwsManager)


@pytest.fixture()
def mock_instance_manager():
    return mock.Mock(spec=InstanceSshManager)


@pytest.fixture()
def rolling_upgrade_manager(mock_aws_manager, mock_instance_manager):
    ssh_config = SshEnvConfig(
        username='test_user',
        private_key_file_path='/path/to/key',
        environment='TestEnv',
        remote_port=22,
        use_bastion_tunnel=False
    )

    return RollingUpgradeManager(
        ssh_config=ssh_config,
        aws_manager=mock_aws_manager,
        instance_manager=mock_instance_manager,
        sleep_time_s=0
    )


def test_rum_looks_for_single_asg(rolling_upgrade_manager, mock_aws_manager):
    mock_aws_manager.find_asg_group.return_value = []
    with pytest.raises(Exception):
        rolling_upgrade_manager.perform_rolling_upgrade_where_needed(
            'env', 'asg-slug')

    mock_aws_manager.find_asg_group.return_value = [{}, {}, {}]
    with pytest.raises(Exception):
        rolling_upgrade_manager.perform_rolling_upgrade_where_needed(
            'env', 'asg-slug')


def test_rum_waits_for_expected_number_of_instances_to_boot(
    rolling_upgrade_manager,
    mock_aws_manager

):
    class Instance:
        private_ip_address = '0.0.0.0'
    mock_aws_manager.get_instances_for_asg.side_effect = (
        [Instance()],
        [Instance()],
        [Instance(), Instance()],
        [Instance(), Instance(), Instance()]
    )

    rolling_upgrade_manager.wait_for_instances('test-asg', 3)

    mock_aws_manager.get_instances_for_asg.assert_has_calls((
        mock.call('test-asg'),
        mock.call('test-asg'),
        mock.call('test-asg'),
        mock.call('test-asg'),
    ))


def test_rum_waits_for_instances_to_be_available(
    rolling_upgrade_manager,
    mock_instance_manager
):
    class Instance:
        private_ip_address = '0.0.0.0'
    mock_instance_manager.is_ready.side_effect = (True, True, False)
    assert not rolling_upgrade_manager.are_all_instances_ready(
        [Instance(), Instance(), Instance()]
    )

    mock_instance_manager.is_ready.side_effect = (True, False, True)
    assert not rolling_upgrade_manager.are_all_instances_ready(
        [Instance(), Instance(), Instance()]
    )

    mock_instance_manager.is_ready.side_effect = (False, True, True)
    assert not rolling_upgrade_manager.are_all_instances_ready(
        [Instance(), Instance(), Instance()]
    )
    mock_instance_manager.is_ready.side_effect = (True, True, True)
    assert rolling_upgrade_manager.are_all_instances_ready(
        [Instance(), Instance(), Instance()]
    )


def test_rum_gets_list_of_instances_to_upgrade(
    rolling_upgrade_manager,
    mock_aws_manager
):
    Instance = namedtuple('Instance', ['id'])

    instances = [Instance('instance1'),
                 Instance('instance2'),
                 Instance('instance3')]
    rolling_upgrade_manager.compare_instance_to_config = mock.Mock()
    mock_aws_manager.get_instances_for_asg.return_value = instances

    rolling_upgrade_manager.compare_instance_to_config.side_effect = \
        ([], [], ['diff'])

    result = rolling_upgrade_manager.get_instances_to_upgrade("test-asg", {})

    assert len(result) == 1
    assert instances[2] in result

    rolling_upgrade_manager.compare_instance_to_config.side_effect = \
        (['diff1', 'diff2'], [], ['diff'])
    result = rolling_upgrade_manager.get_instances_to_upgrade("test-asg", {})

    assert len(result) == 2
    assert instances[0] in result
    assert instances[2] in result

    rolling_upgrade_manager.compare_instance_to_config.side_effect = \
        ([], [], [])
    result = rolling_upgrade_manager.get_instances_to_upgrade("test-asg", {})

    assert len(result) == 0

    mock_aws_manager.get_instances_for_asg.assert_has_calls((
        mock.call('test-asg'),
        mock.call('test-asg'),
        mock.call('test-asg'),
    ))


def test_rum_gets_oldest_instance():
    Instance = namedtuple('Instance', ['launch_time'])

    instances = [
        Instance(launch_time=datetime(2016, 07, 26, 10, 30)),
        Instance(launch_time=datetime(2016, 07, 24, 5, 30)),
        Instance(launch_time=datetime(2016, 07, 26, 11, 0)),
        Instance(launch_time=datetime(2016, 07, 25, 10, 0)),
    ]

    result = RollingUpgradeManager.get_oldest_instance(instances)

    assert result in instances
    assert result == instances[1]
