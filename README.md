# README #

Explicitly managing rolling upgrade of an AWS Auto Scaling Group

### AWS Auto Scaling Group Rolling Upgrade ###

* AWS rolling upgrades terminate instances in a non-deterministic fashion. The best suggestion is that instances are terminated ordered by instance ID, which is a [pseudo?]random hex string. This module manages a rolling upgrade to an AWS auto scaling group by age, terminating the oldest instance first.
* Version 0.1

#### Overview ####

As noted above, AWS Auto Scaling Groups can be configured to perform a rolling upgrade when the launch configuration of the group is updated. AWS does this by terminating instances and replacing them until the whole group has been replaced. AWS wraps some configuration around this to set the minimum number of instances to keep in service and so on. But you can't specify the order in which instances are terminated, and if there's a problem during the rolling upgrade (an instance fails to start, for example) then it's possible that one of the *good* instances is terminated rather than the failing instance; if instances are clustered this can result in the cluster becoming inquorate.

Taking a cluster of three instances as an example, where two running instances are required to maintain a quorum and where we apply a rolling upgrade in a 3 - 2 - 3 - 2 - 3 - 2 - 3 pattern, if an instance fails for whatever reason, then the autoscaling groups rolling upgrade process can terminate one of the two "good" instances and destroy the quorum beyond repair.

We could upgrade using a 3 - 4 - 3 - 4 - 3 - 4 - 3 pattern, but that has a hidden drawback when AWS suddenly terminates an instance to balance instance across AZs while we're busy with some post-deploy provisioning. Ideally we'd be immune to this, but we're not there yet

So the solution is to use CloudFormation to update the auto scaling group's instance configuration but not let it manage the rolling upgrade, and the handle the rolling upgrade ourselves.

### How do I get set up? ###

##### Installation #####
Clone this repository and ensure all the python dependencies by typing:
`pip install -r require,emts.txt`

##### Summary of set up #####
asg_rolling_upgrade.py is normally called directly from the command line, it can be called from a Contiunuous Deployment system, we use Thoughtworks GoCD. If you intend to use GoCD then ensure that Python and the module dependencies are installed on the Go Agent(s).

##### Configuration #####
Once installed, configuration is through command-line arguments and/or environment variables.
The script uses environment variables for:

* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY
* AWS_DEFAULT_REGION

Required arguments are:

* --ssh_tunnel: the ip address of a bastion host to obtain access to the VPC where the instances are deployed
* --ssh_username: a username giving access to the instances
* --ssh_private_key: a private key associated with the ssh_username
* --limit: a string matching the beginning of the target auto scaling group name. This should resolve to a unique auto scaling group.
* --sleep: time to wait (in seconds) between successive tests for completion (defaults to 30)
* --max_wait_attempts: number of times to test for completion (defaults to 40)

##### Using the script #####
The script should be run once CloudFormation has updated the Launch Configuration for the Auto Scaling Group. If instances need configuring to join a cluster this is handled through a separate CD pipeline triggered from the cloud-init mechanism.

For example, assuming an auto scaling group for RabbitMq server instances requires a rolling update, then update the Launch Configuration for the auto scaling group with the updated AMI ID through CloudFormation. Once the Launch Configuration has been updated then running

```python asg_rolling_upgrade.py --ssh_tunnel bastion.smoketest.example.co.uk --ssh_username centos --ssh_private_key ~/.ssh/example.pem --limit SmokeTestRabbitMq```

will

* find the set of running instances in the auto scaling group with a name matching `SmokeTestRabbitMq*` (note the trailing wildcard)
* identify the set of running instances with a launch configuration that does not match the current (updated) launch configuration
* identify the oldest running instance with a launch configuration that does not match the current (updated) launch configuration
* terminate that instance
* wait until the instance is replaced
* repeat until all instances have been replaced

### License ###
<<<What license do we want to use for this?>>>

### Reporting Bugs ###
<<<How do we want this to happen once this is on Github?>>>