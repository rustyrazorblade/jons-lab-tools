import logging

from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook

# Example dags: https://github.com/apache/airflow/tree/providers-amazon/3.2.0/airflow/providers/amazon/aws/example_dags

import datetime

from airflow.sensors.base import PokeReturnValue

from globals import JLT_VPC_LIST, JLT_DEFAULT_REGION, JLT_DEFAULT_VPC

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
    "number_of_instances": Param(3, type="integer", minimum=1, maximum=30),
    "vpc": Param(default_vpc, enum=vpcs)
}


@dag(dag_id=DAG_NAME,
     schedule_interval=None,
     start_date=datetime.datetime(2023, 1, 1),
     tags=["cassandra"],
     params=dag_params)
def provision_cassandra_cluster():
    # new task to create vpc
    @task()
    def create_vpc(params=None):
        region_name = params["region"]
        cluster_name = params["cluster_name"]

        # boto create vpc
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn
        vpc_id = ec2.create_vpc(
            CidrBlock="10.0.0.0/16"
        ).id

        ec2.create_tags(
            Resources=[vpc_id],
            Tags=[{'Key': 'Name', 'Value': cluster_name}]
        )

        return vpc_id

    # taskflow task that creates a VPC subnet
    @task()
    def create_subnet(vpc_id: str, params=None):
        region_name = params["region"]
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn
        subnet = ec2.create_subnet(
            CidrBlock="10.0.0.0/16",
            VpcId=vpc_id
        )
        return subnet.id

    # new task to create security group
    @task()
    def create_security_group(vpc_id: str, params=None):
        region_name = params["region"]
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn

        sg_response = ec2.create_security_group(
            GroupName='my_security_group',
            Description='My security group',
            VpcId=vpc_id
        )
        print(f"Created Security Group: {sg_response}")
        return sg_response.id

    @task()
    def authorize_ingress(security_group_id, params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn
        security_group = ec2.SecurityGroup(security_group_id)
        return security_group.authorize_ingress(
            GroupId=security_group_id,
            IpProtocol='tcp',
            FromPort=22,
            ToPort=22,
            CidrIp='0.0.0.0/0')

    @task()
    def create_ec2_instances(subnet_id, security_group_id, params=None):
        num = params["number_of_instances"]
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn
        instances = ec2.create_instances(
            ImageId='ami-03d390062ea11f660',  # Replace with a valid AMI ID
            MinCount=num,
            MaxCount=num,
            InstanceType=params["instance_type"],
            SecurityGroupIds=[security_group_id],
            SubnetId=subnet_id,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': 'MyInstance'}]
            }]
        )
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

    vpc = create_vpc()
    subnet_id = create_subnet(vpc)
    security_group_id = create_security_group(vpc)
    authorize_ingress(security_group_id)
    instances = create_ec2_instances(subnet_id, security_group_id)
    wait_for_instances(instances)


my_tag = provision_cassandra_cluster()
