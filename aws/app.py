"""
Crackling-cloud AWS

Jacob Bradford (1), Timothy Chappell (1), Brendan Hosking (2), Laurence Wilson (2), Dimitri Perrin (1)
    (1) Queensland University of Technology, Brisbane, Australia 
    (2) Commonwealth Scientific and Industrial Research Organisation (CSIRO), Sydney, Australia 

The standalone edition of the Crackling pipeline is available at https://github.com/bmds-lab/Crackling

"""
import aws_cdk as cdk
import json

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
    aws_s3_notifications as s3n_,
    aws_cloudfront as cloudfront_,
    aws_cloudfront_origins as origins_,
    custom_resources as cr,
    Aws,
    DefaultStackSynthesizer
)     

from constructs import Construct

account_number = Aws.ACCOUNT_ID
availabilityZone = Aws.REGION

class CracklingStack(Stack):
    def __init__(self, scope, id, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ### Virtual Private Cloud
        # VPCs are used for constraining infrastructure to a private network.
        cracklingVpc = ec2_.Vpc(
            scope=self,
            id="CracklingVpc",
            gateway_endpoints={
                "s3" : ec2_.GatewayVpcEndpointOptions(
                    service=ec2_.GatewayVpcEndpointAwsService.S3
                ),
                "DYNAMODB" : ec2_.GatewayVpcEndpointOptions(
                    service=ec2_.GatewayVpcEndpointAwsService.DYNAMODB
                )
            },
          
            # A Network Address Translator routes outbound traffic to the internet when necessary
            nat_gateways=1,
        )

        ### Simple Storage Service (S3) is a object store that can host websites.
        # This bucket is used for hosting the front-end application.
        s3Frontend = s3_.Bucket(self,
            "CracklingWebsite",
            website_index_document="index.html",
            public_read_access=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects = True,
            block_public_access = s3_.BlockPublicAccess.BLOCK_ACLS,
        )

        ### CloudFront is a content delivery network
        # The front-end application will be distributed via CloudFront
        cloudFrontDistribution = cloudfront_.Distribution(self, "CracklingcloudFrontDistribution",
            default_behavior=cloudfront_.BehaviorOptions(origin=origins_.S3Origin(s3Frontend))
        )

        ### Export the CloudFront URL when the Stack has been created
        cloudfront_url = cloudFrontDistribution.distribution_domain_name
        cdk.CfnOutput(self, "Cloudfront_URL", value=cloudfront_url)

        ### Create an S3 bucket to store genome data
        s3Genome = s3_.Bucket(self,
            "genomeStorage", 
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            cors=[s3_.CorsRule(
                allowed_methods=[
                    s3_.HttpMethods.GET,
                    s3_.HttpMethods.POST,
                    s3_.HttpMethods.PUT,
                    s3_.HttpMethods.DELETE,
                    s3_.HttpMethods.HEAD
                ],
                allowed_origins=["*"],
                allowed_headers=[
                    "Authorization",
                    "Content-Type",
                    "x-amz-security-token",
                    "x-amz-date",
                    "x-amz-content-sha256",
                    "Origin",
                    "Accept"
                ],
                max_age=3000
            )]
        )

        ### Delegate permisions to access point
        s3GenomeAccessPointPolicy = iam_.PolicyStatement.from_json({
            "Effect": "Allow",
            "Principal": {
                "AWS": "*"
            },
            "Action": "*",
            "Resource": [
               f"{s3Genome.bucket_arn}",
                f"{s3Genome.bucket_arn}/*"
            ],
            "Condition": {
                "StringEquals": {
                    "s3:DataAccessPointAccount": account_number
                }
            }
        })

        s3Genome.add_to_resource_policy(s3GenomeAccessPointPolicy)
        
        ### VPC access point for Genome storage
        s3GenomeAccess = s3_.CfnAccessPoint(
            scope=self,
            bucket=s3Genome.bucket_name,
            id="s3GenomeAccess",
            vpc_configuration=s3_.CfnAccessPoint.VpcConfigurationProperty(
                vpc_id=cracklingVpc.vpc_id
            )
        )

        lambdaS3AccessPointIAM = iam_.PolicyStatement.from_json({
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": [
                f"{s3GenomeAccess.attr_arn}",
                f"{s3GenomeAccess.attr_arn}/object/*"
            ]
        })

        ### DynamoDB (ddb) is a key-value store.
        # This table stores jobs for processing
        # ddb stores data in partitions
        ddbJobs = ddb_.Table(self, "ddbJobs",
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=ddb_.BillingMode.PAY_PER_REQUEST,
            partition_key=ddb_.Attribute(name="JobID", type=ddb_.AttributeType.STRING),
            stream=ddb_.StreamViewType.NEW_AND_OLD_IMAGES
        )

        ### Stores information on the number of tasks completed by each job
        ddbTaskTracking = ddb_.Table(self, "ddbTaskTracking",
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

        ### Genomes are downloaded from NCBI in portions. This table stores metadata about those portions.
        ddbGenomeParts = ddb_.Table(self, "ddbGenomeParts",
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=ddb_.BillingMode.PAY_PER_REQUEST,
            partition_key=ddb_.Attribute(name="GenomePartFileName", type=ddb_.AttributeType.STRING),
            sort_key=ddb_.Attribute(name="FileNamePartNumber", type=ddb_.AttributeType.NUMBER ),
            stream=ddb_.StreamViewType.NEW_IMAGE
        )

        ### Lambda is an event-driven compute service.
        # Some lambda functions may need additional resources - these are provided via layers.
        # This layer provides the ISSL scoring binary.
        lambdaLayerIssl = lambda_.LayerVersion(self, "isslBinary",
            code=lambda_.Code.from_asset("../layers/isslScoreOfftargets"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64]
        )

        ### Lambda layer containing python3.10 packages for rques
        lambdaLayerRequests = lambda_.LayerVersion(self, "requests",
            code=lambda_.Code.from_asset("../layers/requestsPy310Pkgs"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64],
            compatible_runtimes=[
                lambda_.Runtime.PYTHON_3_10
            ]
        )

        ### Lambda layer containing the sgRNAScorer 2.0 model
        lambdaLayerSgrnascorerModel = lambda_.LayerVersion(self, "sgrnascorer2Model",
            code=lambda_.Code.from_asset("../layers/sgrnascorer2Model"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64],
            compatible_runtimes=[
                lambda_.Runtime.PYTHON_3_10
            ]
        )

        ### Lambda layer containing the RNAfold binary
        lambdaLayerRnafold = lambda_.LayerVersion(self, "rnafold",
            code=lambda_.Code.from_asset("../layers/rnaFold"),
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[lambda_.Architecture.X86_64]
        )

        ### Lambda layer containing shared libraries for compiled binaries
        lambdaLayerLib = lambda_.LayerVersion(self, "lib",
            code=lambda_.Code.from_asset("../layers/lib"),
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
        
        ### Variables used over many lambdas
        ld_library_path = ("/opt/libs:/lib64:/usr/lib64:$LAMBDA_RUNTIME_DIR:$LAMBDA_RUNTIME_DIR/lib:$LAMBDA_TASK_ROOT:$LAMBDA_TASK_ROOT/lib:/opt/lib")
        path = "/usr/local/bin:/usr/bin/:/bin:/opt/bin"
        duration = Duration.minutes(15)

        # Simple Queue Service is a queueing service that enables distributed systems to operate at scale.
        # This queue handles creating ISSL indexes
        sqsIsslCreation = sqs_.Queue(
            self,
            "sqsIsslCreation", 
            receive_message_wait_time=Duration.seconds(1),
            visibility_timeout=duration,
            retention_period=duration
        )

        ### An SQS Deal Letter queue handles messages that have "died" in another queue.
        # This is a dead letter queue for the queue that implements the genome portion/part downloader
        sqsGenomePartDownloads = sqs_.Queue(
            self, "DLQ",
            retention_period=Duration.days(14)
        )

        ### This SQS queue handles downloading genome portions (i.e., files parts)
        sqsGenomeParts = sqs_.Queue(self, "sqsGenomeParts", 
            receive_message_wait_time=Duration.seconds(20),
            visibility_timeout=duration,
            retention_period=Duration.minutes(30),
            dead_letter_queue=sqs_.DeadLetterQueue(
                max_receive_count=3,  # Set maxReceiveCount to 3
                queue=sqsGenomePartDownloads
            )
        )

        ### SQS queue for identifying candidate guides
        # i.e., extracting on-target sites
        sqsTargetScan = sqs_.Queue(
            self,
            "sqsTargetScan", 
            receive_message_wait_time=Duration.seconds(1),
            visibility_timeout=duration,
            retention_period=duration
        )

        ### SQS queue for evaluating off-target risk
        sqsIssl = sqs_.Queue(self, "sqsIssl", 
            receive_message_wait_time=Duration.seconds(20),
            visibility_timeout=duration,
            retention_period=Duration.minutes(30)
        )
        
        ### SQS queue for evaluating guide efficiency
        # The TargetScan lambda function adds guides to this queue for processing
        # The consensus lambda function processes items in this queue
        sqsConsensus = sqs_.Queue(
            self,
            "sqsConsensus", 
            receive_message_wait_time=Duration.seconds(20),
            visibility_timeout=duration,
            retention_period=duration
        )

        ### Lambda function that acts as the entry point to the application.
        # This function creates a record in the DynamoDB jobs table.
        # MAX_SEQ_LENGTH defines the maximum length that the input genetic sequence can be.
        # Read/write permissions on the jobs table needs to be granted to this function.
        lambdaCreateJob = lambda_.Function(self, "createJob", 
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/createJob"),
            layers=[lambdaLayerCommonFuncs],
            vpc=cracklingVpc,
            environment={
                'JOBS_TABLE' : ddbJobs.table_name,
                'MAX_SEQ_LENGTH' : '20000',
                'TASK_TRACKING_TABLE' : ddbTaskTracking.table_name
            }
        )

        ddbJobs.grant_read_write_data(lambdaCreateJob)
        ddbTaskTracking.grant_read_write_data(lambdaCreateJob)

        ### Lambda function that return presigned URL to allow users to upload custom dataset to s3 genome storage
        lambdaCustomDataUpload = lambda_.Function(self, "CustomDataUpload", 
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/customData"),
            layers=[lambdaLayerCommonFuncs],
            vpc=cracklingVpc,
            environment={
                'BUCKET' : s3GenomeAccess.attr_arn,
                'BUCKET_NAME': s3Genome.bucket_name,
                'REGION_NAME': availabilityZone
            }
        )
        s3Genome.grant_read_write(lambdaCustomDataUpload)   
        lambdaCustomDataUpload.add_to_role_policy(lambdaS3AccessPointIAM)

        ### Lambda function that organises the parallel download of genome parts
        # Extracts names and sizes from fasta files in NCBI server
        # Split each file into part file portions
        lambdaGenomeDownloadScheduler = lambda_.Function(self, "downloader", 
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/downloader"),
            layers=[lambdaLayerCommonFuncs,lambdaLayerNcbi,lambdaLayerLib],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 2065,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'BUCKET' : s3GenomeAccess.attr_arn,
                'ISSL_QUEUE' : sqsIsslCreation.queue_url,
                'TARGET_SCAN_QUEUE' : sqsTargetScan.queue_url,
                'FILE_PARTS_QUEUE' : sqsGenomeParts.queue_url,
                'LD_LIBRARY_PATH' : ld_library_path,
                'PATH' : path
            }
        )


        ddbJobs.grant_stream_read(lambdaGenomeDownloadScheduler)
        sqsIsslCreation.grant_send_messages(lambdaGenomeDownloadScheduler)
        sqsTargetScan.grant_send_messages(lambdaGenomeDownloadScheduler)
        sqsGenomeParts.grant_send_messages(lambdaGenomeDownloadScheduler)

        lambdaGenomeDownloadScheduler.add_event_source_mapping(
            "mapLdaDownloaderDdbJobs",
            event_source_arn=ddbJobs.table_stream_arn,
            retry_attempts=0,
            starting_position=lambda_.StartingPosition.LATEST
        )
        s3Genome.grant_read_write(lambdaGenomeDownloadScheduler)   
        lambdaGenomeDownloadScheduler.add_to_role_policy(lambdaS3AccessPointIAM)

       
        ### Lambda function that downloads files from NCBI server and uploads them to S3 
        lambdaGenomePartsDownloader = lambda_.Function(self, "partloader", 
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/partloader"),
            layers=[lambdaLayerCommonFuncs, lambdaLayerRequests],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10), 
            environment={
                'FILES_TABLE' : ddbGenomeParts.table_name,
                'BUCKET' : s3GenomeAccess.attr_arn,
                'ISSL_QUEUE' : sqsIsslCreation.queue_url
            }
        )

        sqsGenomeParts.grant_consume_messages(lambdaGenomePartsDownloader)
        sqsIsslCreation.grant_send_messages(lambdaGenomePartsDownloader)
        ddbGenomeParts.grant_read_write_data(lambdaGenomePartsDownloader)
        s3Genome.grant_read_write(lambdaGenomePartsDownloader)
        lambdaGenomePartsDownloader.add_to_role_policy(lambdaS3AccessPointIAM)

        lambdaGenomePartsDownloader.add_event_source_mapping(
            "mapppIsslCreation",
            event_source_arn=sqsGenomeParts.queue_arn,
            batch_size=1
        )


        # -> -> issl_creation
        lambdaIsslCreation = lambda_.Function(self, "isslCreationLambda", 
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/isslCreation"),
            layers=[lambdaLayerIsslCreation, lambdaLayerCommonFuncs, lambdaLayerLib],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'QUEUE' : sqsTargetScan.queue_url,
                'BUCKET' : s3GenomeAccess.attr_arn,
                'LD_LIBRARY_PATH' : ld_library_path,
                'PATH' : path
            }
        )

        s3Genome.grant_read_write(lambdaIsslCreation)
        sqsIsslCreation.grant_consume_messages(lambdaIsslCreation)
        sqsTargetScan.grant_send_messages(lambdaIsslCreation)
        lambdaIsslCreation.add_event_source_mapping(
            "mapppIsslCreation",
            event_source_arn=sqsIsslCreation.queue_arn,
            batch_size=1
        )
        lambdaIsslCreation.add_to_role_policy(lambdaS3AccessPointIAM)
        
        ### Lambda function that scans a sequence for CRISPR sites.
        # This function is triggered when a record is written to the DynamoDB jobs table.
        # It creates one record per guide in the DynamoDB guides table.
        # It needs permission to read/write data from the jobs and guides tables.
        # It needs permission to send messages to the SQS queues.
        lambdaTargetScan = lambda_.Function(self, "targetScan", 
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/targetScan"),
            layers=[lambdaLayerCommonFuncs],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'TARGETS_TABLE' : ddbTargets.table_name,
                'TASK_TRACKING_TABLE' : ddbTaskTracking.table_name,
                'CONSENSUS_QUEUE' : sqsConsensus.queue_url,
                'ISSL_QUEUE' : sqsIssl.queue_url,
                'LD_LIBRARY_PATH' : ld_library_path,
                'JOBS_TABLE' : ddbJobs.table_name,
                'PATH' : path
            }
        )
        sqsTargetScan.grant_consume_messages(lambdaTargetScan)
        ddbTargets.grant_read_write_data(lambdaTargetScan)
        ddbTaskTracking.grant_read_write_data(lambdaTargetScan)
        ddbJobs.grant_read_write_data(lambdaTargetScan)
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
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/consensus"),
            layers=[lambdaLayerLib, lambdaLayerSgrnascorerModel, lambdaLayerRnafold, lambdaLayerCommonFuncs],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'TARGETS_TABLE' : ddbTargets.table_name,
                'TASK_TRACKING_TABLE' : ddbTaskTracking.table_name,
                'JOBS_TABLE' : ddbJobs.table_name,
                'CONSENSUS_QUEUE' : sqsConsensus.queue_url, 
                'BUCKET' : s3GenomeAccess.attr_arn
            }
        )


        s3Genome.grant_read_write(lambdaConsensus)   
        lambdaConsensus.add_to_role_policy(lambdaS3AccessPointIAM)

        sqsConsensus.grant_consume_messages(lambdaConsensus)
        lambdaConsensus.add_event_source_mapping(
            "mapLdaConsesusSqsConsensus",
            event_source_arn=sqsConsensus.queue_arn,
            batch_size=100,
            max_batching_window=Duration.seconds(10)
        )
        ddbTargets.grant_read_write_data(lambdaConsensus)
        ddbTaskTracking.grant_read_write_data(lambdaConsensus)
        ddbJobs.grant_read_write_data(lambdaConsensus)


        ### Lambda function that assesses guide specificity using ISSL.
        # This function consumes messages in the SQS Issl queue.
        # The results are written to the DynamoDB consensus table.
        lambdaIssl = lambda_.Function(self, "issl", 
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/issl"),
            layers=[lambdaLayerLib, lambdaLayerIssl, lambdaLayerCommonFuncs],
            vpc=cracklingVpc,
            timeout= duration,
            memory_size= 10240,
            ephemeral_storage_size = cdk.Size.gibibytes(10),
            environment={
                'BUCKET' : s3GenomeAccess.attr_arn,
                'TASK_TRACKING_TABLE' : ddbTaskTracking.table_name,
                'TARGETS_TABLE' : ddbTargets.table_name,
                'JOBS_TABLE' : ddbJobs.table_name,
                'ISSL_QUEUE' : sqsIssl.queue_url,
                'LD_LIBRARY_PATH' : ld_library_path,
                'PATH' : path
            }
        )
        sqsIssl.grant_consume_messages(lambdaIssl)
        sqsIssl.grant_send_messages(lambdaIssl)
        lambdaIssl.add_event_source_mapping(
            "mapLdaIsslSqsIssl",
            event_source_arn=sqsIssl.queue_arn,
            batch_size=10, 
            max_batching_window=Duration.seconds(5)
        )
        ddbJobs.grant_read_write_data(lambdaIssl)
        ddbTaskTracking.grant_read_write_data(lambdaIssl)
        ddbTargets.grant_read_write_data(lambdaIssl)
        s3Genome.grant_read_write(lambdaIssl)
        lambdaIssl.add_to_role_policy(lambdaS3AccessPointIAM)

        ### API
        # This handles the staging and deployment of the API. A ClouydFormation output is generated with the API URL.
        # Enable cross-origin resource sharing (CORS).
        apiRest = api_.RestApi(self, 
            "CracklingRestApi",
            default_cors_preflight_options=api_.CorsOptions(
                allow_origins=["*"], 
                 
                # FUTURE ME: Check if having this is overly permissive. Perhaps I configure this just for customUpload
                allow_methods=['GET', 'POST', 'OPTIONS'],  # List allowed methods
                allow_headers=['Content-Type', 'X-Amz-Date', 'Authorization', 'X-Api-Key'] 
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
                                            "dynamodb:GetItem",
                                            "dynamodb:GetRecords",
                                            "dynamodb:Query"
                                        ],
                                        resources=[
                                            ddbTargets.table_arn,
                                            ddbTaskTracking.table_arn  # Include the job status table ARN

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
                                        '       "Start": $targ.Start.N,'
                                        '       "End": $targ.End.N,'
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


        # Path: /jobs/{job-id}/tasks
        apiResourceResultsIdTasks = apiRest.root.add_resource("jobs") \
        .add_resource("{jobid}") \
        .add_resource("tasks")  # returns an `IResource`


        apiResourceResultsIdTasks.add_method(  
            "GET",
            api_.AwsIntegration(
                service="dynamodb",
                action="Query",
                options=api_.IntegrationOptions(
                    credentials_role=iam_.Role(
                        self, "roleApiGetTasksDdb",
                        assumed_by=iam_.ServicePrincipal("apigateway.amazonaws.com"),
                        inline_policies={
                            'readDynamoDB': iam_.PolicyDocument(
                                statements=[
                                    iam_.PolicyStatement(
                                        actions=[
                                            "dynamodb:GetItem",
                                            "dynamodb:GetRecords",
                                            "dynamodb:Query"
                                        ],
                                        resources=[
                                            ddbTaskTracking.table_arn  # Query the tasks table
                                        ],
                                        effect=iam_.Effect.ALLOW
                                    )
                                ]
                            )
                        }
                    ),
                    passthrough_behavior=api_.PassthroughBehavior.WHEN_NO_TEMPLATES,
                    request_templates={
                        'application/json': (
                            '{'
                            f'   "TableName": "{ddbTaskTracking.table_name}",'  # Query for the tasks table
                            '    "KeyConditionExpression": "JobID = :v1",'
                            '    "ExpressionAttributeValues": {'
                            '        ":v1": {"S": "$input.params(\'jobid\')"}'
                            '    }'
                            '}'
                        )
                    },
                    integration_responses=[
                        api_.IntegrationResponse(
                            status_code='200',
                            response_templates={
                                'application/json': (
                                    "#set($allTasks = $input.path('$.Items'))"
                                    '{'
                                    '"recordsTotal": $allTasks.size(),'
                                    '"data": ['
                                    '   #foreach($task in $allTasks) {'
                                    '       "JobID": "$task.JobID.S",'
                                    '       "NumGuides": $task.NumGuides.N,'
                                    '       "NumScoredOntarget": $task.NumScoredOntarget.N,'
                                    '       "NumScoredOfftarget": $task.NumScoredOfftarget.N,'
                                    '       "Version": $task.Version.N'
                                    '   }#if($foreach.hasNext),#end'
                                    '   #end'
                                    ']'
                                    '}'
                                )
                            },
                            response_parameters={
                                'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key'",
                                'method.response.header.Access-Control-Allow-Methods': "'POST,OPTIONS'",
                                'method.response.header.Access-Control-Allow-Origin': "'*'"
                            },
                        )
                    ]
                )
            ),
            request_parameters={
                'method.request.path.proxy': True
            },
            method_responses=[
                api_.MethodResponse(
                    response_models={
                        'application/json': api_.Model.EMPTY_MODEL
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
        apiResourceSubmitJob = apiRest.root.add_resource("submit")
        apiResourceSubmitJob.add_method(
            "POST",
            api_.LambdaIntegration(lambdaCreateJob)
        )


        # /customUpload
        apiResourceUploadData = apiRest.root.add_resource("customUpload")
        apiResourceUploadData.add_method(
            "GET",
            api_.LambdaIntegration(lambdaCustomDataUpload)
        )


        apiRest_url = apiRest.url  # Make sure this variable is defined before use


        ### The frontend contains a placeholder for the API URL
        # This Lambda function is invoked when the Stack is created or updated
        lambdaUpdateFrontendWithApiUrl = lambda_.Function(
            self, "lambdaUpdateFrontendWithApiUrl",
            runtime=lambda_.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/updateApiUrl"),
            vpc=cracklingVpc,
            environment={
                "BUCKET_NAME": s3Frontend.bucket_name,
                "OBJECT_KEY": "index.html",
                "NEW_API_URL": apiRest_url,
                "CLOUDFRONT_DISTRIBUTION_ID": cloudFrontDistribution.distribution_id
            }, 
        )

        s3Frontend.grant_read_write(lambdaUpdateFrontendWithApiUrl)

        s3FrontendDeploy = s3d_.BucketDeployment(
            self, "DeployFrontend",
            sources=[s3d_.Source.asset("../frontend")],
            destination_bucket=s3Frontend,
            distribution=cloudFrontDistribution,
            distribution_paths=["/*"], # this will invalidate everything in the CloudFront distribution
            retain_on_delete=False
        )

        # S3 Event Notification to trigger Lambda on index.html update
        s3Frontend.add_event_notification(
            s3_.EventType.OBJECT_CREATED,
            s3n_.LambdaDestination(lambdaUpdateFrontendWithApiUrl),
            s3_.NotificationKeyFilter(prefix="index.html")  # Only trigger for "index.html"
        )
              
        update_resource = cr.AwsCustomResource(self, "UpdateHtmlResource",
            on_create={
                "service": "Lambda",
                "action": "invoke",
                "parameters": {
                    "FunctionName": lambdaUpdateFrontendWithApiUrl.function_arn,
                },
                "physical_resource_id": cr.PhysicalResourceId.of("UpdateHtmlResource")
            },

            on_update={
                "service": "Lambda",
                "action": "invoke",
                "parameters": {
                    "FunctionName": lambdaUpdateFrontendWithApiUrl.function_arn,
                }, 
                "physical_resource_id": cr.PhysicalResourceId.of("UpdateHtmlResource")
            },

            policy=cr.AwsCustomResourcePolicy.from_statements([
                iam_.PolicyStatement(
                    actions=["lambda:InvokeFunction"],
                    resources=[lambdaUpdateFrontendWithApiUrl.function_arn],
                ), 
                iam_.PolicyStatement(
                    actions=["cloudfront:CreateInvalidation"],
                    resources=["*"],
                )
            ])
        )

        update_resource.node.add_dependency(s3FrontendDeploy)
        



app = cdk.App()

CracklingStack(app, f"CracklingStack", synthesizer=DefaultStackSynthesizer(
    #file_assets_bucket_name="a-public-facing-bucket-n10753753"
))

# CracklingStack(
#     app, 
#     "CracklingStackWithAssetBucketParam",
#     synthesizer=DefaultStackSynthesizer(
#         file_assets_bucket_name=cdk.CfnParameter(app, "FileAssetsBucketName", type="String").value_as_string
#     )
# )

app.synth()
