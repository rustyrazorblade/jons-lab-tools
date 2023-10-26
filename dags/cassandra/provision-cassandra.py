from airflow import DAG
from airflow.example_dags.subdags.subdag import subdag
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from airflow.decorators import task

from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2StartInstanceOperator,
)

import datetime

DAG_NAME = "provision-cassandra"
image_id = ""


@task
def parse_response(instance_ids: list):
    return instance_ids[0]


# https://instances.vantage.sh/?min_memory=32&min_vcpus=8&min_storage=100&region=us-west-2&selected=c5d.xlarge,r6gd.large
with DAG(
        dag_id=DAG_NAME,
        default_args={"retries": 2},
        tags=["cassandra"],
        start_date=datetime.datetime(2023, 1, 1),
        params={
            "instance_type": Param("c5d.xlarge",
                                   enum=["c5d.xlarge"],
                                   values_display={
                                       "c5d.xlarge": "c5.xlarge (8GB, 4CPU, 1x100GB NVMe)",
                                   }),
            "number_of_instances": Param(3, type="integer", minimum=1, maximum=30),
            "cluster_name": Param("test", type="string", maxLength=30),
            "ami": Param("ami-0fc5d935ebf8bc3bc", type="string")
        }
) as dag:
    # test_context = sys_test_context_task()

    key_name = "test"
    instance_name = f"cassandra-instance"
    instance_id = "instance_id"

    config = {
        "InstanceType": "t4g.micro",
        "KeyName": key_name,
        "TagSpecifications": [
            {"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": instance_name}]}
        ],
        # Use IMDSv2 for greater security, see the following doc for more details:
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
        "MetadataOptions": {"HttpEndpoint": "enabled", "HttpTokens": "required"},
    }

    create_instance = EC2CreateInstanceOperator(
        task_id="create_instance",
        image_id="{{ params.ami }}",
        max_count=1,
        min_count=1,
        config=config,
    )

    create_instance.wait_for_completion = True

    start_instance = EC2StartInstanceOperator(
        task_id="start_instance",
        instance_id=instance_id,
    )

    await_instance = EC2InstanceStateSensor(
        task_id="await_instance",
        instance_id=instance_id,
        target_state="running",
    )

    create_instance >> start_instance >> await_instance
