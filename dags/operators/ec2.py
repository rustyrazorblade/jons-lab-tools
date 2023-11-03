from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook


class CreateVPCOperator(BaseOperator):
    """
    Creates a Virtual Private Cloud (VPC) in AWS.

    :param aws_conn_id: The connection ID to use when connecting to AWS.
    :type aws_conn_id: str
    :param vpc_name: The name for the VPC.
    :type vpc_name: str
    :param cidr_block: The IPv4 network range for the VPC in CIDR notation.
    :type cidr_block: str
    """

    def __init__(self,
                 vpc_name: str,
                 aws_conn_id: str = "aws_default",
                 region_name: str = "us-west-2",
                 cidr_block="10.0.0.0/16",
                 **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.vpc_name = vpc_name
        self.cidr_block = cidr_block
        self.region_name = region_name

    def boto(self) -> BaseAwsConnection:
        ec2_hook = EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        return ec2_hook.conn

    def execute(self, context):
        self.log.info(f'Creating VPC with name: {self.vpc_name} and CIDR block: {self.cidr_block}')

        ec2 = self.boto()
        self.log.info(f"EC2 {type(ec2)}")

        vpc = ec2.create_vpc(
            CidrBlock=self.cidr_block
        )

        vpc_id = vpc.id

        ec2.create_tags(
            Resources=[vpc_id],
            Tags=[{'Key': 'Name', 'Value': self.vpc_name}]
        )

        return vpc_id
