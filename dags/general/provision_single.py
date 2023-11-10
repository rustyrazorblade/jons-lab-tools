import datetime

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from general.common import get_vpc, get_security_group, get_subnets

from globals import JLT_DEFAULT_REGION, JLT_DEFAULT_BASE_AMI, get_default_vpc_option, get_vpc_options

DAG_NAME = "provision_single_instance"

dag_params = {
    "region": JLT_DEFAULT_REGION,
    "ami": JLT_DEFAULT_BASE_AMI,
    "vpc": Param(get_default_vpc_option(), enum=get_vpc_options())
}


@dag(dag_id=DAG_NAME,
     schedule=None,
     start_date=datetime.datetime(2023, 1, 1),
     tags=["aws"],
     params=dag_params)
def provision_single_instance():
    @task()
    def provision_instance(params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn
        vpc = ec2.Vpc(params.vpc)
        instances = ec2.create_instances(
            ImageId=JLT_DEFAULT_BASE_AMI,  # Replace with a valid AMI ID
            MinCount=1,
            MaxCount=1,
            InstanceType=params["instance_type"],
            SecurityGroupIds=params.security_group_id,
            # SubnetId=subnet,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': 'MyInstance'}]
            }])
        return list(instances)[0]

    vpc = get_vpc()



provision_single_instance()
