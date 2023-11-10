from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook

# Example dags: https://github.com/apache/airflow/tree/providers-amazon/3.2.0/airflow/providers/amazon/aws/example_dags

import datetime

from airflow.sensors.base import PokeReturnValue

from general.common import get_ec2
from globals import JLT_DEFAULT_VPC, JLT_DEFAULT_REGION, DagValues

# :param api_type: If set to ``client_type`` then hook use ``boto3.client("ec2")`` capabilities,
# If set to ``resource_type`` then hook use ``boto3.resource("ec2")`` capabilities.

DAG_NAME = "provision_vpc"

dag_params = {
    "vpc_name": JLT_DEFAULT_VPC,
    "region": JLT_DEFAULT_REGION
}


@dag(dag_id=DAG_NAME,
     schedule_interval=None,
     start_date=datetime.datetime(2023, 1, 1),
     tags=["cassandra"],
     params=dag_params)
def provision_vpc():
    # new task to create vpc
    @task()
    def create_vpc(params=None):
        region_name = params["region"]

        # boto create vpc
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn
        vpc_id = ec2.create_vpc(
            CidrBlock="10.0.0.0/16"
        ).id

        ec2.create_tags(
            Resources=[vpc_id],
            Tags=[{'Key': 'Name', 'Value': params["vpc_name"]}]
        )

        return vpc_id

    @task.sensor(poke_interval=30, timeout=600, mode="reschedule")
    def wait_for_vpc(vpc_id, params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn
        vpc = ec2.Vpc(vpc_id)
        vpc.wait_until_available()
        return PokeReturnValue(is_done=True, xcom_value=vpc_id)

    # taskflow task that creates a VPC subnet
    # todo make it work with one subnet per AZ
    @task()
    def create_subnet(vpc_id: str, cidr: str, az: str, params=None):
        region_name = params["region"]
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=region_name).conn
        subnet = ec2.create_subnet(
            CidrBlock=cidr,
            VpcId=vpc_id,
            AvailabilityZone=az,
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

    @task
    def create_internet_gateway(params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn
        igw_response = ec2.create_internet_gateway()
        return igw_response.id

    @task
    def attach_internet_gateway(vpc_id, igw_gateway_id, params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn
        vpc = ec2.Vpc(vpc_id)
        vpc.attach_internet_gateway(InternetGatewayId=igw_gateway_id)
        print(f"Internet Gateway {igw_gateway_id} attached to VPC {vpc_id} successfully.")

    @task
    def create_routing_table(vpc_id, params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn
        route_table = ec2.create_route_table(VpcId=vpc_id)
        return route_table.id

    @task
    def create_route(route_table_id, igw_id, params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn
        route_table = ec2.RouteTable(route_table_id)
        route = route_table.create_route(
            DestinationCidrBlock='0.0.0.0/0',
            GatewayId=igw_id
        )
        return None

    @task
    def associate_subnet(subnet_id, route_table_id, params=None):
        ec2 = EC2Hook(aws_conn_id="aws_default", region_name=params["region"]).conn
        route_table = ec2.RouteTable(route_table_id)
        route_table.associate_with_subnet(SubnetId=subnet_id)

    vpc = wait_for_vpc(create_vpc())
    routing_table = create_routing_table(vpc)

    subnet = {}
    azs = ["us-west-2a", "us-west-2b", "us-west-2c"]

    for idx, az in enumerate(azs):
        subnet[az] = create_subnet(vpc,  f'10.0.{idx}.0/24', az)
        associate_subnet(subnet[az], routing_table)
    #     ec2_client.associate_route_table(SubnetId=subnet_id, RouteTableId=route_table_id)

    security_group_id = create_security_group(vpc)
    authorize_ingress(security_group_id)

    # internet gateway
    igw_gateway_id = create_internet_gateway()
    attach_internet_gateway(vpc, igw_gateway_id)

    # routing table
    create_route(routing_table, igw_gateway_id)


    # new task that accepts a tag name and fetches all instances with that tag
    @task()
    def get_instances_by_tag(tag_name: str, tag_value: str, params=None):
        ec2 = get_ec2(params)
        instances = ec2.instances.filter(
            Filters=[{'Name': f'tag:{tag_name}', 'Values': [tag_value]}])
        # return a list of named tuples for the instances id and tag name
        return [(instance.id, instance.tags[0]['Value']) for instance in instances]

my_tag = provision_vpc()
