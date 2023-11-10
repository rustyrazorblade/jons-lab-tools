import datetime

from airflow.decorators import dag

from general.common import get_vpc, get_security_group, \
    get_subnets, provision_instance, wait_for_instance

from globals import DagParams, get_dag_params

DAG_NAME = "provision_single_instance"

dag_params = get_dag_params(DagParams.NAME,
                            DagParams.REGION,
                            DagParams.UBUNTU_AMI,
                            DagParams.VPC,
                            DagParams.INSTANCE_TYPE)


@dag(dag_id=DAG_NAME,
     schedule=None,
     start_date=datetime.datetime(2023, 1, 1),
     tags=["aws"],
     params=dag_params)
def provision_single_instance():
    vpc = get_vpc()
    subnets = get_subnets(vpc)
    security_group = get_security_group(vpc)
    instance_id = provision_instance(security_group, subnets)
    wait_for_instance(instance_id)


provision_single_instance()
