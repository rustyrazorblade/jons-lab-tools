import logging

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.sensors.base import PokeReturnValue

from globals import DagParams, DagValues


def get_ec2(params):
    return EC2Hook(aws_conn_id="aws_default",
                   region_name=params[DagParams.REGION.value]).conn


"""
Works in tandem with the DagParams.VPC parameter
"""
@task()
def get_vpc(params=None):
    dag = DagValues(params)
    logging.info("Fetching VPCs in %s", dag.region)
    ec2 = get_ec2(params)

    filters = [{'Name': 'tag:Name', 'Values': [dag.vpc]}]
    vpc = list(ec2.vpcs.filter(Filters=filters))[0]
    return vpc.id


@task()
def get_subnets(vpc_id, params=None):
    # get all the subnets in the vpc
    ec2 = get_ec2(params)
    vpc = ec2.Vpc(vpc_id)
    subnets = vpc.subnets.all()
    return [subnet.id for subnet in subnets]


@task()
def get_security_group(vpc_id, params=None):
    ec2 = get_ec2(params)
    vpc = ec2.Vpc(vpc_id)
    logging.info(f"Fetching security groups for {vpc}")
    vps_security_groups = list(vpc.security_groups.all())
    logging.info(f"SGs fetched {vps_security_groups}")
    return vps_security_groups[0].id


@task()
def provision_instance(security_group_id, subnets, tags=None, params=None):
    if tags is None:
        tags = dict()

    dag = DagValues(params)
    ec2 = get_ec2(params)
    subnet_id = subnets[0]
    instances = ec2.create_instances(
        ImageId=dag.ubuntu_ami,
        MinCount=1,
        MaxCount=1,
        InstanceType=dag.instance_type,
        SecurityGroupIds=[security_group_id],
        SubnetId=subnet_id,
        KeyName=dag.key_pair,
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{'Key': 'Name', 'Value': 'MyInstance'}]
        }])

    instance = list(instances)[0]
    ipv6_addresses = []
    for network_interface in instance.network_interfaces_attribute:
        for ipv6_address_info in network_interface.get('Ipv6Addresses', []):
            ipv6_addresses.append(ipv6_address_info['Ipv6Address'])
    logging.info("Created instance %s with address %s", instance.id, ipv6_addresses)
    return instance.id


@task.sensor(poke_interval=30, timeout=600, mode="reschedule")
def wait_for_instance(instance_id, params=None):
    ec2 = get_ec2(params)

    instance = ec2.Instance(instance_id)
    if instance.state["Code"] != 16:  # 16 is running, 0 = pending
        return PokeReturnValue(is_done=False)

    return PokeReturnValue(is_done=True)


# new task that accepts a tag name and fetches all instances with that tag
@task()
def get_instances_by_tag(tag_name: str, tag_value: str, params=None):
    ec2 = get_ec2(params)
    instances = ec2.instances.filter(
        Filters=[{'Name': f'tag:{tag_name}', 'Values': [tag_value]}])
    # return a list of named tuples for the instances id and tag name
    return [(instance.id, instance.tags[0]['Value']) for instance in instances]