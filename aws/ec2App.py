from email.policy import default
from platform import machine
from xml.dom import UserDataHandler
import aws_cdk as cdk

from aws_cdk import (
    Stack,
    aws_ec2 as ec2_,
    aws_lambda as lambda_,
    aws_apigateway as api_,
    aws_sqs as sqs_,
    aws_dynamodb as ddb_,
    aws_iam as iam_,
    aws_s3 as s3_,
    aws_s3_deployment as s3d_,
    aws_s3_notifications as s3_notify,
)

class CracklingStack(Stack):
    def __init__(self, scope, id, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        cracklingVpc = ec2_.Vpc.from_lookup(self, "CracklingVpc", vpc_id="vpc-9d849bfa")
        self.instance_name = 'EC2forAMI'
        instance = ec2_.InstanceType.of(ec2_.InstanceClass.R5AD, ec2_.InstanceSize.XLARGE2)

        secuirtyGroup = ec2_.SecurityGroup(self, 'ec2-sec-grp',
            vpc=cracklingVpc,
            description="Allow ssh access to ec2 instances",
            allow_all_outbound=True)

        secuirtyGroup.add_ingress_rule(
            ec2_.Peer.any_ipv4(),ec2_.Port.tcp(22)
        )

        userData = ec2_.UserData.for_linux()
        userData.add_commands('apt-get update -y',
            'apt-get install -y git awscli ec2-instance-connect',
            'until git clone https://github.com/aws-quickstart/quickstart-linux-utilities.git; do echo "Retrying"; done',
            'cd /quickstart-linux-utilities',
            'source quickstart-cfn-tools.source',
            'qs_update-os || qs_err',
            'qs_bootstrap_pip || qs_err',
            'qs_aws-cfn-bootstrap || qs_err',
            'mkdir -p /opt/aws/bin',
            'ln -s /usr/local/bin/cfn-* /opt/aws/bin/')


        linux = ec2_.MachineImage.from_ssm_parameter(
            '/aws/service/canonical/ubuntu/server/focal/stable/current/amd64/hvm/ebs-gp2/ami-id',
            user_data=userData
            )

        ec2_inst = ec2_.Instance(
            self, 'EC2forAMI',
            instance_name=self.instance_name,
            instance_type=instance,
            vpc=cracklingVpc,
            machine_image=linux,
            key_name="NickDownloader-ISSL-EC2Key",
            security_group=secuirtyGroup,
            init=ec2_.CloudFormationInit.from_elements(
            ec2_.InitFile.from_file_inline("/ec2Code/modules/bt2Lambda.py", "../modules/bowtie2/lambda_function.py"),
            ec2_.InitFile.from_file_inline("/ec2Code/modules/common_funcs.py", "../layers/commonFuncs/python/common_funcs.py"),
            ec2_.InitFile.from_file_inline("/ec2Code/modules/issl_creation.py", "../modules/isslCreation/lambda_function.py"),
            ec2_.InitFile.from_file_inline("/ec2Code/modules/lambda_downloader.py", "../modules/downloader/lambda_function.py"),
            ec2_.InitFile.from_file_inline("/ec2Code/ec2_ncbi.py", "../modules/ec2_ncbi/ec2_ncbi.py"),
            ec2_.InitFile.from_file_inline("/init.sh", "../scripts/init.sh"),
            ec2_.InitFile.from_file_inline("/ec2Code/src/ISSL/isslCreateIndex", "../layers/isslCreation/ISSL/isslCreateIndex",base64_encoded=True),
            ec2_.InitCommand.shell_command("bash /init.sh"),
            ec2_.InitCommand.shell_command("rm /init.sh"))            
        )

app = cdk.App()
env_AU = cdk.Environment(account="377188290550", region="ap-southeast-2")
CracklingStack(app,"CracklingStackEC2", env= env_AU)
app.synth()    
