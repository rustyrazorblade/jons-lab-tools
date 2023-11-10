import datetime

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.sensors.base import PokeReturnValue

from general.common import get_vpc, get_security_group, get_subnets

from globals import DagParams, get_dag_params

DAG_NAME = "provision_single_instance"

dag_params = get_dag_params(DagParams.REGION,
                            DagParams.UBUNTU_AMI,
                            DagParams.VPC,
                            DagParams.INSTANCE_TYPE)


@dag(dag_id=DAG_NAME,
     schedule=None,
     start_date=datetime.datetime(2023, 1, 1),
     tags=["aws"],
     params=dag_params)
def provision_single_instance():
    @task()
    def provision_instance(security_group_id, subnets, params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params[DagParams.REGION.value]).conn
        subnet_id = subnets[0]
        instances = ec2.create_instances(
            ImageId=params[DagParams.UBUNTU_AMI.value],  # Replace with a valid AMI ID
            MinCount=1,
            MaxCount=1,
            InstanceType=params["instance_type"],
            SecurityGroupIds=[security_group_id],
            SubnetId=subnet_id,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': 'MyInstance'}]
            }])
        return list(instances)[0].id

    @task.sensor(poke_interval=30, timeout=600, mode="reschedule")
    def wait_for_instance(instance_id, params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params[DagParams.REGION.value]).conn

        instance = ec2.Instance(instance_id)
        if instance.state["Code"] != 16:  # 16 is running, 0 = pending
            return PokeReturnValue(is_done=False)

        return PokeReturnValue(is_done=True)

    vpc = get_vpc()
    subnets = get_subnets(vpc)
    security_group = get_security_group(vpc)
    instance_id = provision_instance(security_group, subnets)
    wait_for_instance(instance_id)


provision_single_instance()
