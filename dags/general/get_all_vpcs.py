import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.models import Variable

from globals import JLT_VPC_LIST, JLT_DEFAULT_REGION

DAG_NAME = "GetAllVPCs"
DAG_PARAMS = {
    "region": JLT_DEFAULT_REGION
}


@dag(dag_id=DAG_NAME,
     schedule_interval=None,
     start_date=datetime(2023, 1, 1),
     tags=["aws"],
     params=DAG_PARAMS)
def get_all_vpcs():
    @task
    def get_and_store_all_vpcs(params=None):
        region_name = params["region"]
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn
        vpc_names = []
        vpcs = ec2.vpcs.all()
        for vpc in vpcs:
            logging.info("VPC: %s", vpc)
            # A fallback name if the VPC doesn't have a Name tag
            vpc_name = 'default'
            # Extract name from tags if it exists
            for tag in vpc.tags or []:
                if tag['Key'] == 'Name':
                    vpc_name = tag['Value']
                    break
            vpc_names.append(vpc_name)
            Variable.set(JLT_VPC_LIST, vpc_names, serialize_json=True)

    get_and_store_all_vpcs()


vpcs = get_all_vpcs()
