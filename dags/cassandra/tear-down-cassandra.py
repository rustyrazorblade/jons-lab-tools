from airflow import DAG
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from airflow.decorators import task

from airflow.providers.amazon.aws.operators.ec2 import (
    EC2StopInstanceOperator,
    EC2TerminateInstanceOperator,
)

import datetime

DAG_NAME = "teardown-cassandra-cluster"
instance_id = "xxx"

with DAG(
        dag_id=DAG_NAME,
        tags=["cassandra"],
        schedule_interval=None,
        params={

        },
        start_date=datetime.datetime(2023, 1, 1)):

    stop_instance = EC2StopInstanceOperator(
        task_id="stop_instance",
        instance_id=instance_id,
    )

    terminate_instance = EC2TerminateInstanceOperator(
        task_id="terminate_instance",
        instance_ids=instance_id,
        wait_for_completion=True,
    )

    stop_instance >> terminate_instance
