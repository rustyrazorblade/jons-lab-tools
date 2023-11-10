from datetime import datetime, timedelta

from airflow.decorators import dag, task

from general.common import get_ec2, get_vpc
from globals import DagParams, get_dag_params, DagValues

dag_params = get_dag_params(DagParams.VPC, DagParams.REGION)
DAG_NAME = "delete_vpc"


@dag(dag_id=DAG_NAME,
     schedule=None,
     tags=["aws"],
     start_date=datetime(2023, 1, 1),
     params=dag_params)
def delete_vpc():
    @task
    def detach_delete_internet_gateways(vpc_id, params=None):
        ec2 = get_ec2(params)
        dag_values = DagValues(params)
        vpc = ec2.Vpc(dag_values.vpc)
        for gw in vpc.internet_gateways.all():
            vpc.detach_internet_gateway(InternetGatewayId=gw.id)
            gw.delete()
        return vpc_id

    @task
    def delete_subnets(vpc_id, params=None):
        ec2 = get_ec2(params)
        vpc = ec2.Vpc(vpc_id)
        for subnet in vpc.subnets.all():
            subnet.delete()
        return vpc_id

    @task
    def delete_route_tables(vpc_id, params=None):
        ec2 = get_ec2(params)
        vpc = ec2.Vpc(vpc_id)
        for rt in vpc.route_tables.all():
            if not rt.associations:
                rt.delete()
        return vpc_id

    @task
    def delete_security_groups(vpc_id, params=None):
        ec2 = get_ec2(params)
        vpc = ec2.Vpc(vpc_id)
        for sg in vpc.security_groups.all():
            if sg.group_name != 'default':
                sg.delete()
        return vpc_id

    @task
    def delete_network_acls(vpc_id, params=None):
        ec2 = get_ec2(params)
        vpc = ec2.Vpc(vpc_id)
        for acl in vpc.network_acls.all():
            if not acl.is_default:
                acl.delete()
        return vpc_id

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def delete_vpc(vpc_id, params=None):
        ec2 = get_ec2(params)
        vpc = ec2.Vpc(vpc_id)
        vpc.delete()

    # Define task dependencies
    vpc_id = get_vpc()
    vpc_id_after_gws = detach_delete_internet_gateways(vpc_id)
    vpc_id_after_subnets = delete_subnets(vpc_id_after_gws)
    vpc_id_after_route_tables = delete_route_tables(vpc_id_after_subnets)
    vpc_id_after_security_groups = delete_security_groups(vpc_id_after_route_tables)
    vpc_id_after_acls = delete_network_acls(vpc_id_after_security_groups)
    delete_vpc(vpc_id_after_acls)

delete_vpc()
