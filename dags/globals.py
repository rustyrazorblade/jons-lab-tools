import logging
from enum import Enum
import datetime
from airflow.models import Variable, Param

JLT_VPC_LIST = "jlt_vpc_list"
JLT_DEFAULT_VPC = "jlt"
JLT_DEFAULT_REGION = "us-west-2"
JLT_DEFAULT_BASE_AMI = 'ami-03d390062ea11f660'


# deprecated
def get_vpc_options():
    return Variable.get(JLT_VPC_LIST, default_var=[], deserialize_json=True)


def get_default_vpc_option():
    JLT_VPCS = get_vpc_options()
    return JLT_DEFAULT_VPC if JLT_DEFAULT_VPC in JLT_VPCS else JLT_VPCS[0]


class DagParams(Enum):
    CLUSTER_NAME = "cluster_name"
    INSTANCE_TYPE = "instance_type"
    REGION = "region"
    NUMBER_OF_INSTANCES = "number_of_instances"
    # VPC Picker
    VPC = "vpc"
    UBUNTU_AMI = "ubuntu_ami"
    BASE_AMI = "base_ami"
    NAME = "name"
    KEY_PAIR = "key_pair"


class DagValues:
    def __init__(self, params):
        self.cluster_name = params.get(DagParams.CLUSTER_NAME.value, None)
        self.instance_type = params.get(DagParams.INSTANCE_TYPE.value, None)
        self.region = params.get(DagParams.REGION.value, None)
        self.number_of_instances = params.get(DagParams.NUMBER_OF_INSTANCES.value, None)
        self.vpc = params.get(DagParams.VPC.value, None)
        self.ubuntu_ami = params.get(DagParams.UBUNTU_AMI.value, None)
        self.name = params.get(DagParams.NAME.value, None)
        self.key_pair = params.get(DagParams.KEY_PAIR.value, None)


class Tag(Enum):
    TYPE = "type"


class InstanceType(Enum):
    CASSANDRA = "cassandra"
    KAFKA = "kafka"


def get_default_dag_params():
    return {
        DagParams.CLUSTER_NAME: Param("test" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")),
        DagParams.INSTANCE_TYPE: Param("c5d.xlarge",
                                       enum=["c5d.xlarge"],
                                       values_display={
                                           "c5d.xlarge": "c5.xlarge (8GB, 4CPU, 1x100GB NVMe)",
                                       }),
        DagParams.REGION: JLT_DEFAULT_REGION,
        DagParams.NUMBER_OF_INSTANCES: Param(3, type="integer", enum=[3, 6, 9, 12]),
        DagParams.VPC: Param(get_default_vpc_option(), enum=get_vpc_options()),
        DagParams.UBUNTU_AMI: Param(JLT_DEFAULT_BASE_AMI,
                                    enum=[JLT_DEFAULT_BASE_AMI],
                                    values_display={
                                        JLT_DEFAULT_BASE_AMI: "Ubuntu 22"
                                    }),
        DagParams.NAME: Param("test_" + datetime.datetime.now().strftime("%H_%M")),
        DagParams.KEY_PAIR: Param("laptop2022")
    }


def get_dag_params(*args: DagParams):
    d = get_default_dag_params()
    logging.info("Getting dag params for args: %s", args)
    tmp = {key.value: d[key] for key in args if key in d.keys()}
    logging.info("dag params: %s", tmp)
    return tmp
