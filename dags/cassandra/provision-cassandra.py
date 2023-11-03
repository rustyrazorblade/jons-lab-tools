from airflow.decorators import dag, task
from airflow import DAG
from airflow.models import Param
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from operators.ec2 import CreateVPCOperator

from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2StartInstanceOperator
)

# Example dags: https://github.com/apache/airflow/tree/providers-amazon/3.2.0/airflow/providers/amazon/aws/example_dags

import datetime

DAG_NAME = "provision-cassandra-cluster"

# https://instances.vantage.sh/?min_memory=32&min_vcpus=8&min_storage=100&region=us-west-2&selected=c5d.xlarge,r6gd.large
with DAG(
        dag_id=DAG_NAME,
        default_args={"retries": 2},
        tags=["cassandra"],
        start_date=datetime.datetime(2023, 1, 1),
        schedule_interval=None,
        render_template_as_native_obj=True,
        params={
            "instance_type": Param("c5d.xlarge",
                                   enum=["c5d.xlarge"],
                                   values_display={
                                       "c5d.xlarge": "c5.xlarge (8GB, 4CPU, 1x100GB NVMe)",
                                   }),
            "region": Param("us-west-2",
                            enum=["us-west-2"]),
            "number_of_instances": Param(3, type="integer", minimum=1, maximum=30),
            "cluster_name": Param("test", type="string", maxLength=30),
            "ami": Param("ami-099650fa48ff54238", type="string"),
            "key_name": Param("rrboct23", enum=["rrboct23"])
        }
) as dag:
    # test_context = sys_test_context_task()

    key_name = "test"
    instance_name = f"cassandra-instance"
    instance_id = "instance_id"

    config = {
        "InstanceType": "{{ params.instance_type }}",
        "KeyName": key_name,
        "TagSpecifications": [
            {"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": instance_name}]}
        ],
        # Use IMDSv2 for greater security, see the following doc for more details:
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
        "MetadataOptions": {"HttpEndpoint": "enabled", "HttpTokens": "required"},
    }

    # create VPC
    create_vpc = CreateVPCOperator(
        task_id="create_vpc",
        vpc_name="test",
        retries=0,
    )


    # create security group

    create_instance = EC2CreateInstanceOperator(
        task_id="create_instance",
        image_id="{{ params.ami }}",
        max_count="{{ params.number_of_instances }}",
        min_count="{{ params.number_of_instances }}",
        config=config,
        region_name="{{ params.region }}",
        # config={"VpcId": create_vpc.}
    )

    create_instance.wait_for_completion = True

    start_instance = EC2StartInstanceOperator(
        task_id="start_instance",
        instance_id=instance_id,
        region_name="{{ params.region }}"
    )

    start_instance.wait_for_completion = True

    await_instance = EC2InstanceStateSensor(
        task_id="await_instance",
        instance_id=instance_id,
        target_state="running",
        region_name="{{ params.region }}"
    )

    create_vpc >> create_instance >> start_instance >> await_instance

