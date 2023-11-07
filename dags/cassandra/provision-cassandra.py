import logging

from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook

# Example dags: https://github.com/apache/airflow/tree/providers-amazon/3.2.0/airflow/providers/amazon/aws/example_dags

import datetime

from airflow.sensors.base import PokeReturnValue

from globals import JLT_VPC_LIST, JLT_DEFAULT_REGION, JLT_DEFAULT_VPC, JLT_DEFAULT_BASE_AMI

DAG_NAME = "provision-cassandra-cluster"

vpcs = Variable.get(JLT_VPC_LIST, deserialize_json=True)
default_vpc = JLT_DEFAULT_VPC if JLT_DEFAULT_VPC in vpcs else vpcs[0]

dag_params = {
    "cluster_name": Param("test" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")),
    "instance_type": Param("c5d.xlarge",
                           enum=["c5d.xlarge"],
                           values_display={
                               "c5d.xlarge": "c5.xlarge (8GB, 4CPU, 1x100GB NVMe)",
                           }),
    "region": JLT_DEFAULT_REGION,
    "number_of_instances": Param(3, type="integer", enum=[3, 6, 9, 12]),
    "vpc": Param(default_vpc, enum=vpcs)
}


@dag(dag_id=DAG_NAME,
     schedule_interval=None,
     start_date=datetime.datetime(2023, 1, 1),
     tags=["cassandra"],
     params=dag_params)
def provision_cassandra_cluster():
    # new task to create vpc
    # new task to create security group

    @task
    def get_vpc(params=None):
        region_name = params["region"]
        vpc_name = params["vpc"]
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn
        filters = [{'Name': 'tag:Name', 'Values': [vpc_name]}]
        vpc = list(ec2.vpcs.filter(Filters=filters))[0]
        return vpc.id

    @task()
    def get_subnets(vpc_id, params=None):
        # get all the subnets in the vpc
        region_name = params["region"]
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn
        vpc = ec2.Vpc(vpc_id)
        subnets = vpc.subnets.all()
        return [subnet.id for subnet in subnets]

    @task()
    def get_security_group(vpc_id, params=None):
        region_name = params["region"]
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn
        vpc = ec2.Vpc(vpc_id)
        logging.info(f"Fetching security groups for {vpc}")
        vps_security_groups = list(vpc.security_groups.all())
        logging.info(f"SGs fetched {vps_security_groups}")
        return vps_security_groups[0].id

    @task()
    def create_ec2_instances(subnet, security_group_id, params=None):
        # spread the instances across the subnets
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn
        num = int(params["number_of_instances"] / 3)

        instances = ec2.create_instances(
            ImageId=JLT_DEFAULT_BASE_AMI,  # Replace with a valid AMI ID
            MinCount=num,
            MaxCount=num,
            InstanceType=params["instance_type"],
            SecurityGroupIds=[security_group_id],
            SubnetId=subnet,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': 'MyInstance'}]
            }])

        instance_ids = [instance.id for instance in instances]
        print(f"Created EC2 Instances: {instance_ids}")
        return instance_ids

    @task.sensor(poke_interval=30, timeout=600, mode="reschedule")
    def wait_for_instances(instances, params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn

        for instance_id in instances:
            instance = ec2.Instance(instance_id)
            if instance.state["Code"] != 16:  # 16 is running, 0 = pending
                return PokeReturnValue(is_done=False)

        return PokeReturnValue(is_done=True)

    vpc = get_vpc()
    subnets = get_subnets(vpc)
    sg = get_security_group(vpc)
    instances = create_ec2_instances.partial(security_group_id=sg).expand(subnet=subnets)
    wait_for_instances.expand(instances=instances)


my_tag = provision_cassandra_cluster()
