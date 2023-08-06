#!/usr/bin/env python3

"""
Crackling-cloud AWS

Jacob Bradford (1), Timothy Chappell (1), Brendan Hosking (2), Laurence Wilson (2), Dimitri Perrin (1)
    (1) Queensland University of Technology, Brisbane, Australia 
    (2) Commonwealth Scientific and Industrial Research Organisation (CSIRO), Sydney, Australia 

The standalone edition of the Crackling pipeline is available at https://github.com/bmds-lab/Crackling

"""
import aws_cdk as cdk

from aws_cdk import (
    Duration,
    RemovalPolicy,
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

version = "-Dev-1-v1"

class CracklingStack(Stack):
    def __init__(self, scope, id, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        

        ### Virtual Private Cloud
        # VPCs are used for constraining infrastructure to a private network.
        cracklingVpc = ec2_.Vpc(
            scope=self,
            id=f"CracklingVpc{version}",
            vpc_name=f"CracklingVpc{version}",

            #add s3 gateway
            gateway_endpoints={
                "s3" : ec2_.GatewayVpcEndpointOptions(
                    service=ec2_.GatewayVpcEndpointAwsService.S3
                ),
                "DYNAMODB" : ec2_.GatewayVpcEndpointOptions(
                    service=ec2_.GatewayVpcEndpointAwsService.DYNAMODB
                )
            }
            ,nat_gateways=0
        )

        ### Simple Storage Service (S3) is a key-object store that can host websites.
        # This bucket is used for hosting the front-end application.
        s3Frontend = s3_.Bucket(self,
            "CracklingWebsite",
            website_index_document="index.html",
            public_read_access=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects = True,
            block_public_access = s3_.BlockPublicAccess.BLOCK_ACLS,
        )
        s3FrontendDeploy = s3d_.BucketDeployment(
            self, "DeployFrontend",
            sources=[
                s3d_.Source.asset("../frontend")
            ],
            destination_bucket=s3Frontend,
            
            # destination_key_prefix="web/static",
            retain_on_delete=False
        )
        cdk.CfnOutput(self, "S3_Frontend_URL", value=s3Frontend.bucket_website_url)
        
        # New S3 Bucket for Genome File storage
        s3Genome = s3_.Bucket(self,
            "genomeStorage", 
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects = True
        )

        # VPC access point for Genome storage
        s3GenomeAccess = s3_.CfnAccessPoint(
            scope=self,
            bucket=s3Genome.bucket_name,
            id="s3GenomeAccess",
            vpc_configuration=s3_.CfnAccessPoint.VpcConfigurationProperty(
                vpc_id=cracklingVpc.vpc_id
            )
        )

        #New S3 Bucket for Log storage
        s3Log = s3_.Bucket(self, "logStorage")    

        ### DynamoDB (ddb) is a key-value store.
        # This table stores jobs for processing
        # ddb stores data in partitions
        ddbJobs = ddb_.Table(self, "ddbJobs",
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=ddb_.BillingMode.PAY_PER_REQUEST,
            partition_key=ddb_.Attribute(name="JobID", type=ddb_.AttributeType.STRING),
            stream=ddb_.StreamViewType.NEW_AND_OLD_IMAGES
        )

        ### DynamoDB table for storing targets.
        # The sort key enables quicker indexing.
        ddbTargets = ddb_.Table(self, "ddbTargets",
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=ddb_.BillingMode.PAY_PER_REQUEST,
            partition_key=ddb_.Attribute(name="JobID", type=ddb_.AttributeType.STRING),
            sort_key=ddb_.Attribute(name="TargetID", type=ddb_.AttributeType.NUMBER),
            stream=ddb_.StreamViewType.NEW_AND_OLD_IMAGES
        )

        ### Lambda is an event-driven compute service.
        # Some lambda functions may need additional resources - these are provided via layers.
        # This layer provides the ISSL scoring binary.
        lambdaLayerIssl = lambda_.LayerVersion(self, "isslBinary",
            code=lambda_.Code.from_asset("../layers/isslScoreOfftargets"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64]
        )

        ### Lambda layer containing python3.8 packages
        lambdaLayerPythonPkgs = lambda_.LayerVersion(self, "python38pkgs",
            code=lambda_.Code.from_asset("../layers/consensusPy38Pkgs"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64],
            compatible_runtimes=[
                lambda_.Runtime.PYTHON_3_8
            ]
        )

        ### Lambda layer containing the sgRNAScorer 2.0 model
        lambdaLayerSgrnascorerModel = lambda_.LayerVersion(self, "sgrnascorer2Model",
            code=lambda_.Code.from_asset("../layers/sgrnascorer2Model"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64],
            compatible_runtimes=[
                lambda_.Runtime.PYTHON_3_8
            ]
        )

        ### Lambda layer containing the RNAfold binary
        lambdaLayerRnafold = lambda_.LayerVersion(self, "rnafold",
            code=lambda_.Code.from_asset("../layers/rnaFold"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64]
        )

        ### Lambda layer contaiing shared libraries for compiled binaries
        lambdaLayerLib = lambda_.LayerVersion(self, "lib",
            code=lambda_.Code.from_asset("../layers/lib"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64]
        )
        
        ### Layers required for downloader and assoc. layers, explained in ../layers/README.md
        # This layer provides the bowtie2 "binaries"/script files
        lambdaLayerBt2Bin = lambda_.LayerVersion(self, "bt2Bin",
            code=lambda_.Code.from_asset("../layers/bt2Bin"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64]
        )
        ### This layer provides an updated version of the libstdc++ library required for bowtie2
        lambdaLayerBt2Lib = lambda_.LayerVersion(self, "bt2Lib",
            code=lambda_.Code.from_asset("../layers/bt2Lib"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64]
        )
        ### This layer contains a python module of commonly used functions across the lambdas
        lambdaLayerCommonFuncs = lambda_.LayerVersion(self, "commonFuncs",
            code=lambda_.Code.from_asset("../layers/commonFuncs"),
            removal_policy=RemovalPolicy.DESTROY
        )
        ### Layer containing ncbi.datasets module and dependencies
        lambdaLayerNcbi = lambda_.LayerVersion(self, "ncbi",
            code=lambda_.Code.from_asset("../layers/ncbi"),
            removal_policy=RemovalPolicy.DESTROY
        )
        ### Layer containing the python script and binary required for building issl indices
        lambdaLayerIsslCreation = lambda_.LayerVersion(self, "isslCreationLayer",
            code=lambda_.Code.from_asset("../layers/isslCreation"),
            removal_policy=RemovalPolicy.DESTROY
        )

        ### Lambda function that acts as the entry point to the application.
        # This function creates a record in the DynamoDB jobs table.
        # MAX_SEQ_LENGTH defines the maximum length that the input genetic sequence can be.
        # Read/write permissions on the jobs table needs to be granted to this function.
        lambdaCreateJob = lambda_.Function(self, "createJob", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            insights_version = lambda_.LambdaInsightsVersion.VERSION_1_0_98_0,
            code=lambda_.Code.from_asset("../modules/createJob"),
            layers=[lambdaLayerCommonFuncs, lambdaLayerPythonPkgs],
            vpc=cracklingVpc,# was this meant to be left commented
            environment={
                'JOBS_TABLE' : ddbJobs.table_name,
                'MAX_SEQ_LENGTH' : '20000',
                'LOG_BUCKET': s3Log.bucket_name
            }
        )
        ddbJobs.grant_read_write_data(lambdaCreateJob)
        s3Log.grant_read_write(lambdaCreateJob)
        
        #Variables used over many lambdas
        ld_library_path = ("/opt/libs:/lib64:/usr/lib64:$LAMBDA_RUNTIME_DIR:"
        "$LAMBDA_RUNTIME_DIR/lib:$LAMBDA_TASK_ROOT:$LAMBDA_TASK_ROOT/lib:/opt/lib")
        path = "/usr/local/bin:/usr/bin/:/bin:/opt/bin"
        duration = Duration.minutes(15)
        
        # -> SQS queues
        sqsDownload = sqs_.Queue(self, "sqsDownload", 
            receive_message_wait_time=Duration.seconds(1),
            visibility_timeout=duration,
            retention_period=duration
        )
        sqsBowtie2 = sqs_.Queue(self, "sqsBowtie2", 
            receive_message_wait_time=Duration.seconds(1),
            visibility_timeout=duration,
            retention_period=duration
        )
        sqsIsslCreaton = sqs_.Queue(self, "sqsIsslCreaton", 
            receive_message_wait_time=Duration.seconds(1),
            visibility_timeout=duration,
            retention_period=duration
        )
        sqsTargetScan = sqs_.Queue(self, "sqsTargetScan", 
            receive_message_wait_time=Duration.seconds(1),
            visibility_timeout=duration,
            retention_period=duration
        )
        sqsIssl = sqs_.Queue(self, "sqsIssl", 
            receive_message_wait_time=Duration.seconds(1),
            visibility_timeout=duration,
            retention_period=duration
        )
        ### SQS queue for evaluating guide efficiency
        # The TargetScan lambda function adds guides to this queue for processing
        # The consensus lambda function processes items in this queue
        sqsConsensus = sqs_.Queue(self, "sqsConsensus", 
            receive_message_wait_time=Duration.seconds(20),
            visibility_timeout=duration,
            retention_period=duration
        )

        # IAM role and surrounding instance profile for scheduler to create EC2 
        # instance if genome is above "EC2_CUTOFF" threshold
        ec2role = iam_.Role(self, "ec2role",
            assumed_by=iam_.ServicePrincipal("ec2.amazonaws.com"),
            description="Example role..."
        )
        ec2role.add_to_policy(iam_.PolicyStatement(
            actions=["ec2:*"],
            resources=["*"]
        ))
        ec2role.add_to_policy(iam_.PolicyStatement(
            actions=["s3:*"],
            resources=["*"]
        ))
        cfn_instance_profile = iam_.CfnInstanceProfile(self, "MyCfnInstanceProfile",
            roles=[ec2role.role_name]
        )

        # Lambda Scheduler
        lambdaScheduler = lambda_.Function(self, "scheduler", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            insights_version = lambda_.LambdaInsightsVersion.VERSION_1_0_98_0,
            code=lambda_.Code.from_asset("../modules/scheduler"),
            layers=[lambdaLayerCommonFuncs,lambdaLayerNcbi,lambdaLayerLib],
            vpc=cracklingVpc,
            timeout= duration,
            environment={
                'QUEUE' : sqsDownload.queue_url,
                'LD_LIBRARY_PATH' : ld_library_path,
                'PATH' : path,
                'BUCKET' : s3Genome.bucket_name,
                'GENOME_ACCESS_POINT_ARN' : s3GenomeAccess.attr_arn,
                "AMI": "ami-0a3394674772b58a3",
                "INSTANCE_TYPE": "r5ad.2xlarge",
                "EC2_ARN" : cfn_instance_profile.attr_arn,
                "REGION" : "ap-southeast-2",
                "EC2_CUTOFF" : str(650),
                "LOG_BUCKET": s3Log.bucket_name
            }
        )
        
       
        lambdaScheduler.role.add_to_principal_policy(iam_.PolicyStatement(
            actions=["ec2:RunInstances"],
            resources=["*"]
        ))
        lambdaScheduler.role.add_to_principal_policy(iam_.PolicyStatement(
            actions=["s3:*"],
            resources=["*"]
        ))
        lambdaScheduler.role.add_to_principal_policy(iam_.PolicyStatement(
            actions=["iam:*"],
            resources=["*"]
        ))
        
                
        s3Genome.grant_read_write(lambdaScheduler)
        s3Log.grant_read_write(lambdaScheduler)
        ddbJobs.grant_stream_read(lambdaScheduler)
        sqsDownload.grant_send_messages(lambdaScheduler)
        lambdaScheduler.add_event_source_mapping(
            "mapLdaSchedulerDdbJobs",
            event_source_arn=ddbJobs.table_stream_arn,
            retry_attempts=0,
            starting_position=lambda_.StartingPosition.LATEST
        )
        
        # Lambda Downloader
        lambdaDownloader = lambda_.Function(self, "downloader", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            insights_version = lambda_.LambdaInsightsVersion.VERSION_1_0_98_0,
            code=lambda_.Code.from_asset("../modules/downloader"),
            layers=[lambdaLayerCommonFuncs,lambdaLayerNcbi,lambdaLayerLib],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'JOBS_TABLE' : ddbJobs.table_name,
                'MAX_SEQ_LENGTH' : '20000',
                'BUCKET' : s3Genome.bucket_name,
                'GENOME_ACCESS_POINT_ARN' : s3GenomeAccess.attr_arn,
                'ISSL_QUEUE' : sqsIsslCreaton.queue_url,
                'BT2_QUEUE' : sqsBowtie2.queue_url,
                'LD_LIBRARY_PATH' : ld_library_path,
                'PATH' : path,
                'LOG_BUCKET': s3Log.bucket_name
            }
        )
        
        
        ddbJobs.grant_read_write_data(lambdaDownloader)
        sqsIsslCreaton.grant_send_messages(lambdaDownloader)
        sqsBowtie2.grant_send_messages(lambdaDownloader)
        sqsDownload.grant_consume_messages(lambdaDownloader)
        lambdaDownloader.add_event_source_mapping(
            "mapLdaSqsDownload",
            event_source_arn=sqsDownload.queue_arn,
            batch_size=1
        )
        s3Genome.grant_read_write(lambdaDownloader)   
        s3Log.grant_read_write(lambdaDownloader)      

        # -> -> bt2
        lambdaBowtie2 = lambda_.Function(self, "bowtie2", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            insights_version = lambda_.LambdaInsightsVersion.VERSION_1_0_98_0,
            code=lambda_.Code.from_asset("../modules/bowtie2"),
            layers=[lambdaLayerBt2Lib, lambdaLayerBt2Bin, lambdaLayerCommonFuncs,lambdaLayerLib],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'BUCKET' : s3Genome.bucket_name,
                'GENOME_ACCESS_POINT_ARN' : s3GenomeAccess.attr_arn,
                'LD_LIBRARY_PATH' : ld_library_path,
                'PATH' : path,
                'LOG_BUCKET': s3Log.bucket_name
            }
        )
        s3Genome.grant_read_write(lambdaBowtie2)
        s3Log.grant_read_write(lambdaBowtie2)
        sqsBowtie2.grant_consume_messages(lambdaBowtie2)
        lambdaBowtie2.add_event_source_mapping(
            "mapLdaSqsBowtie",
            event_source_arn=sqsBowtie2.queue_arn,
            batch_size=1
        )

        # -> -> issl_creation
        lambdaIsslCreation = lambda_.Function(self, "isslCreationLambda", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            insights_version = lambda_.LambdaInsightsVersion.VERSION_1_0_98_0,
            code=lambda_.Code.from_asset("../modules/isslCreation"),
            layers=[lambdaLayerIsslCreation, lambdaLayerCommonFuncs, lambdaLayerLib],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'BUCKET' : s3Genome.bucket_name,
                'GENOME_ACCESS_POINT_ARN' : s3GenomeAccess.attr_arn,
                'LD_LIBRARY_PATH' : ld_library_path,
                'PATH' : path,
                'LOG_BUCKET': s3Log.bucket_name
            }
        )

        s3Genome.grant_read_write(lambdaIsslCreation)
        s3Log.grant_read_write(lambdaIsslCreation)
        sqsIsslCreaton.grant_consume_messages(lambdaIsslCreation)
        lambdaIsslCreation.add_event_source_mapping(
            "mapppIsslCreation",
            event_source_arn=sqsIsslCreaton.queue_arn,
            batch_size=1
        )

        # s3-triggered lambda to SQS to targetScan
        lambdaS3Check = lambda_.Function(self, "s3Check", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            insights_version = lambda_.LambdaInsightsVersion.VERSION_1_0_98_0,
            code=lambda_.Code.from_asset("../modules/s3Check"),
            layers=[lambdaLayerCommonFuncs],
            vpc=cracklingVpc,
            timeout= duration,
            environment={
                'QUEUE' : sqsTargetScan.queue_url,
                'LD_LIBRARY_PATH' : ld_library_path,
                'PATH' : path, 
                'LOG_BUCKET': s3Log.bucket_name
            }
        )
        
        s3Genome.grant_read_write(lambdaS3Check)
        s3Log.grant_read_write(lambdaS3Check)
        # Create trigger for Lambda function using suffix
        notification = s3_notify.LambdaDestination(lambdaS3Check)
        notification.bind(self, s3Genome)        
        # Add Create Event only for .jpg files
        s3Genome.add_object_created_notification(
           notification, s3_.NotificationKeyFilter(suffix='.notif'))
        sqsTargetScan.grant_send_messages(lambdaS3Check)

        ### Lambda function that scans a sequence for CRISPR sites.
        # This function is triggered when a record is written to the DynamoDB jobs table.
        # It creates one record per guide in the DynamoDB guides table.
        # It needs permission to read/write data from the jobs and guides tables.
        # It needs permission to send messages to the SQS queues.
        lambdaTargetScan = lambda_.Function(self, "targetScan", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            insights_version = lambda_.LambdaInsightsVersion.VERSION_1_0_98_0,
            code=lambda_.Code.from_asset("../modules/targetScan"),
            layers=[lambdaLayerPythonPkgs,lambdaLayerCommonFuncs],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'TARGETS_TABLE' : ddbTargets.table_name,
                'CONSENSUS_QUEUE' : sqsConsensus.queue_url,
                'ISSL_QUEUE' : sqsIssl.queue_url,
                'LD_LIBRARY_PATH' : ld_library_path,
                'PATH' : path,
                'LOG_BUCKET': s3Log.bucket_name
            }
        )
        
        s3Log.grant_read_write(lambdaTargetScan)
        sqsTargetScan.grant_consume_messages(lambdaTargetScan)
        ddbTargets.grant_read_write_data(lambdaTargetScan)
        sqsConsensus.grant_send_messages(lambdaTargetScan)
        sqsIssl.grant_send_messages(lambdaTargetScan)
        lambdaTargetScan.add_event_source_mapping(
            "mapSqsTargetScan",
            event_source_arn=sqsTargetScan.queue_arn,
            batch_size=1
        )        

        ### Lambda function to assess guide efficiency
        # This function consumes messages in the SQS consensus queue.
        # The results are written to the DynamoDB consensus table.
        lambdaConsensus = lambda_.Function(self, "consensus", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            insights_version = lambda_.LambdaInsightsVersion.VERSION_1_0_98_0,
            code=lambda_.Code.from_asset("../modules/consensus"),
            layers=[lambdaLayerLib, lambdaLayerPythonPkgs, lambdaLayerSgrnascorerModel, lambdaLayerRnafold],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'TARGETS_TABLE' : ddbTargets.table_name,
                'CONSENSUS_QUEUE' : sqsConsensus.queue_url,
                'LOG_BUCKET': s3Log.bucket_name
            }
        )
        s3Log.grant_read_write(lambdaConsensus)
        sqsConsensus.grant_consume_messages(lambdaConsensus)
        lambdaConsensus.add_event_source_mapping(
            "mapLdaConsesusSqsConsensus",
            event_source_arn=sqsConsensus.queue_arn,
            batch_size=100,
            max_batching_window=Duration.seconds(1)
        )
        ddbTargets.grant_read_write_data(lambdaConsensus)


        ### Lambda function that assesses guide specificity using ISSL.
        # This function consumes messages in the SQS Issl queue.
        # The results are written to the DynamoDB consensus table.
        lambdaIssl = lambda_.Function(self, "issl", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            insights_version = lambda_.LambdaInsightsVersion.VERSION_1_0_98_0,
            code=lambda_.Code.from_asset("../modules/issl"),
            layers=[lambdaLayerLib, lambdaLayerIssl, lambdaLayerCommonFuncs],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'BUCKET' : s3Genome.bucket_name,
                'GENOME_ACCESS_POINT_ARN' : s3GenomeAccess.attr_arn,
                'TARGETS_TABLE' : ddbTargets.table_name,
                'JOBS_TABLE' : ddbJobs.table_name,
                'ISSL_QUEUE' : sqsIssl.queue_url,
                'LD_LIBRARY_PATH' : ld_library_path,
                'PATH' : path,
                'LOG_BUCKET': s3Log.bucket_name
            }
        )
        sqsIssl.grant_consume_messages(lambdaIssl)
        lambdaIssl.add_event_source_mapping(
            "mapLdaIsslSqsIssl",
            event_source_arn=sqsIssl.queue_arn,
            batch_size=10
        )
        ddbJobs.grant_read_write_data(lambdaIssl)
        ddbTargets.grant_read_write_data(lambdaIssl)
        s3Genome.grant_read_write(lambdaIssl)
        s3Log.grant_read_write(lambdaIssl)

        ### API
        # This handles the staging and deployment of the API. A CloudFormation output is generated with the API URL.
        # Enable cross-origin resource sharing (CORS).
        apiRest = api_.RestApi(self, 
            "CracklingRestApi",
            default_cors_preflight_options=api_.CorsOptions(
                allow_origins=['*']
            ),
            deploy_options=api_.StageOptions(
                logging_level=api_.MethodLoggingLevel.ERROR,
                metrics_enabled=True
            )
        ) 
         
        # Path: /results/{job-id}/targets
        apiResourceResultsIdTargets = apiRest.root.add_resource("results") \
            .add_resource("{jobid}") \
            .add_resource("targets") # returns an `IResource`
            
        # Add a method to the above path.
        # This method has a custom IAM role to allow it to read the dynamodb targets table.
        # The integration response (from dynamodb) is transformed using a Apache Velocity template.
        # This is probably the most difficult part of the Stack to understand.
        # You should read about the concepts of AWS ApiGateway: https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-basic-concept.html
        #   Particularly focus on: integration request, integration response, method request, method response
        apiResourceResultsIdTargets.add_method( # Adds a `Method` object
            "GET",
            api_.AwsIntegration( 
                service="dynamodb",
                action="Query",
                options=api_.IntegrationOptions(
                    credentials_role=iam_.Role(
                        self, "roleApiGetTargetsDdb",
                        assumed_by=iam_.ServicePrincipal("apigateway.amazonaws.com"),
                        inline_policies={
                            'readDynamoDB' : iam_.PolicyDocument(
                                statements=[
                                    iam_.PolicyStatement(
                                        actions=[
                                            "dynamodb:GetIem",
                                            "dynamodb:GetRecords",
                                            "dynamodb:Query"
                                        ],
                                        resources=[
                                            ddbTargets.table_arn
                                        ],
                                        effect=iam_.Effect.ALLOW
                                    )
                                ]
                            )
                        }
                    ),
                    passthrough_behavior=api_.PassthroughBehavior.WHEN_NO_TEMPLATES,
                    request_templates={
                        'application/json' : (
                             '{'
                            f'    "TableName": "{ddbTargets.table_name}",'
                             '    "KeyConditionExpression": "JobID = :v1",'
                             '    "ExpressionAttributeValues": {'
                             '        ":v1": {'
                             '            "S": "$input.params(\'jobid\')"'
                             '       }'
                             '    }'
                             '}'
                        )
                    },
                    integration_responses=[
                        api_.IntegrationResponse(
                            status_code='200',
                            response_templates={
                                    'application/json' : (
                                        "#set($allTargs = $input.path('$.Items'))"
                                        '{'
                                        '"recordsTotal": $allTargs.size(),'
                                        '"data" : ['
                                        '   #foreach($targ in $allTargs) {'
                                        '       "Sequence": "$targ.Sequence.S",'
                                        '       "Start": "$targ.Start.N",'
                                        '       "End": "$targ.End.N",'
                                        '       "Strand": "$targ.Strand.S",'
                                        '       "Consensus": "$targ.Consensus.S",'
                                        '       "IsslScore": "$targ.IsslScore.S"'
                                        '   }#if($foreach.hasNext),#end'
                                        '   #end'
                                        ']'
                                        '}'
                                    )
                            },
                            response_parameters={
                                # double quote the values in this dict, as per the documentation:
                                #   "You must enclose static values in single quotation marks"
                                #   https://docs.aws.amazon.com/cdk/api/v1/python/aws_cdk.aws_apigateway/IntegrationResponse.html#aws_cdk.aws_apigateway.IntegrationResponse.response_parameters
                                'method.response.header.Access-Control-Allow-Headers' : "'Content-Type,X-Amz-Date,Authorization,X-Api-Key'",
                                'method.response.header.Access-Control-Allow-Methods' : "'POST,OPTIONS'",
                                'method.response.header.Access-Control-Allow-Origin'  : "'*'"
                            },
                        )
                    ]
                )
            ),
            request_parameters={
                'method.request.path.proxy' : True
            },
            method_responses=[
                api_.MethodResponse(
                    response_models={
                        'application/json' : api_.Model.EMPTY_MODEL
                    },
                    response_parameters={
                        'method.response.header.Access-Control-Allow-Headers': True,
                        'method.response.header.Access-Control-Allow-Methods': True,
                        'method.response.header.Access-Control-Allow-Origin': True
                    },
                    status_code='200'
                )
            ]
        )

        # /submit
        apiResourceSubmitJob = apiRest.root.add_resource("submit") # returns an `IResource`
        apiResourceSubmitJob.add_method( # Adds a `Method` object
            "POST",
            api_.LambdaIntegration(lambdaCreateJob)
        )


app = cdk.App()
CracklingStack(app, f"CracklingStack-{version}")
app.synth()
