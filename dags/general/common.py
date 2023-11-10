import logging

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook

from globals import DagParams


@task()
def get_vpc(params=None):
    logging.info("Fetching VPCs in %s", params[DagParams.REGION.value])
    ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn

    filters = [{'Name': 'tag:Name', 'Values': [params[DagParams.VPC.value]]}]
    vpc = list(ec2.vpcs.filter(Filters=filters))[0]
    return vpc.id


@task()
def get_subnets(vpc_id, params=None):
    # get all the subnets in the vpc
    region_name = params[DagParams.REGION.value]
    ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn
    vpc = ec2.Vpc(vpc_id)
    subnets = vpc.subnets.all()
    return [subnet.id for subnet in subnets]


@task()
def get_security_group(vpc_id, params=None):
    region_name = params[DagParams.REGION.value]
    ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn
    vpc = ec2.Vpc(vpc_id)
    logging.info(f"Fetching security groups for {vpc}")
    vps_security_groups = list(vpc.security_groups.all())
    logging.info(f"SGs fetched {vps_security_groups}")
    return vps_security_groups[0].id
