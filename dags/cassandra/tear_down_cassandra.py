from airflow import DAG
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from airflow.decorators import task, dag

import datetime

from globals import DagParams, get_dag_params

DAG_NAME = "teardown_cassandra_cluster"
instance_id = "xxx"

@dag(DAG_NAME,
     schedule_interval=None,
     start_date=datetime.datetime(2023, 1, 1),
     tags=["cassandra", "aws"],
     params=get_dag_params(DagParams.VPC))
def teardown_cassandra_cluster():
    @task()
    def get_instances(vpc_id):
        pass

    @task()
    def get_instance_id():
        return instance_id

    @task()
    def stop_instance(instance_id):
        pass

    @task()
    def terminate_instance(instance_id):
        pass

    instance_id = get_instance_id()
    stop_instance(instance_id)
    terminate_instance(instance_id)
