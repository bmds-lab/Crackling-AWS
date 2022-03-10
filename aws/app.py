#!/usr/bin/env python3

"""
Crackling-cloud AWS

Author: Jake Bradford, and with thanks to our colleagues in the Transformational Bioinformatics lab
at the CSIRO, including Denis Bauer, Laurence Wilson and Brendan Hosking.

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
    aws_dynamodb as ddb_
)

class CracklingStack(Stack):
    def __init__(self, scope, id, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ### Virtual Private Cloud
        # VPCs are used for constraining infrastructure to a private network.
        cracklingVpc = ec2_.Vpc(self, "CracklingVpc")

        ### Simple Queue Service (SQS) is a queuing service for serverless applications.
        # This queue is for off-target scoring.
        # The TargetScan lambda function adds guides to this queue for processing
        # The Issl lambda function processes items in this queue
        sqsIssl = sqs_.Queue(self, "sqsIssl", 
            receive_message_wait_time=Duration.seconds(20)
        )

        ### SQS queue for evaluating guide efficiency
        # The TargetScan lambda function adds guides to this queue for processing
        # The consensus lambda function processes items in this queue
        sqsConsensus = sqs_.Queue(self, "sqsConsensus", 
            receive_message_wait_time=Duration.seconds(20)
        )

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

        ### Lambda layer containing ISSL indexes
        lambdaLayerIsslIdxs = lambda_.LayerVersion(self, "isslIndexes",
            code=lambda_.Code.from_asset("../layers/isslIndexes"),
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

        ### Lambda function that acts as the entry point to the application.
        # This function creates a record in the DynamoDB jobs table.
        # MAX_SEQ_LENGTH defines the maximum length that the input genetic sequence can be.
        # Read/write permissions on the jobs table needs to be granted to this function.
        lambdaCreateJob = lambda_.Function(self, "createJob", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/createJob"),
            layers=[lambdaLayerPythonPkgs],
            #vpc=cracklingVpc,
            environment={
                'JOBS_TABLE' : ddbJobs.table_name,
                'MAX_SEQ_LENGTH' : '20000'
            }
        )
        ddbJobs.grant_read_write_data(lambdaCreateJob)

        ### Lambda function that scans a sequence for CRISPR sites.
        # This function is triggered when a record is written to the DynamoDB jobs table.
        # It creates one record per guide in the DynamoDB guides table.
        # It needs permission to read/write data from the jobs and guides tables.
        # It needs permission to send messages to the SQS queues.
        lambdaTargetScan = lambda_.Function(self, "targetScan", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/targetScan"),
            layers=[lambdaLayerPythonPkgs],
            vpc=cracklingVpc,
            environment={
                'TARGETS_TABLE' : ddbTargets.table_name,
                'CONSENSUS_QUEUE' : sqsConsensus.queue_url,
                'ISSL_QUEUE' : sqsIssl.queue_url
            }
        )
        ddbTargets.grant_read_write_data(lambdaTargetScan)
        ddbJobs.grant_stream_read(lambdaTargetScan)
        sqsConsensus.grant_send_messages(lambdaTargetScan)
        sqsIssl.grant_send_messages(lambdaTargetScan)
        lambdaTargetScan.add_event_source_mapping(
            "mapLdaTargetScanDdbJobs",
            event_source_arn=ddbJobs.table_stream_arn,
            retry_attempts=100,
            starting_position=lambda_.StartingPosition.TRIM_HORIZON
        )
        

        ### Lambda function to assess guide efficiency
        # This function consumes messages in the SQS consensus queue.
        # The results are written to the DynamoDB consensus table.
        lambdaConsensus = lambda_.Function(self, "consensus", 
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("../modules/consensus"),
            layers=[lambdaLayerLib, lambdaLayerPythonPkgs, lambdaLayerSgrnascorerModel, lambdaLayerRnafold],
            vpc=cracklingVpc,
            environment={
                'TARGETS_TABLE' : ddbTargets.table_name,
                'CONSENSUS_QUEUE' : sqsConsensus.queue_url
            }
        )
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
            code=lambda_.Code.from_asset("../modules/issl"),
            layers=[lambdaLayerLib, lambdaLayerIssl, lambdaLayerIsslIdxs],
            vpc=cracklingVpc,
            environment={
                'TARGETS_TABLE' : ddbTargets.table_name,
                'JOBS_TABLE' : ddbJobs.table_name,
                'ISSL_QUEUE' : sqsIssl.queue_url
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

        ### API
        # this handles the staging and deployment of the API. An CloudFormation output is generated with the API URL.
        
        # Get the S3 bucket that hosts the frontend website
        #s3Frontend = s3_.Bucket.from_bucket

        apiRest = api_.RestApi(self, 
            "RestApi",
            default_cors_preflight_options=api_.CorsOptions(
                allow_origins=['*']
            )
        ) 
         
        # /results/{job-id}/targets
        apiResourceResultsIdTargets = apiRest.root.add_resource("results") \
            .add_resource("{jobid}") \
            .add_resource("targets") # returns an `IResource`
            
        # # TODO: this probably needs fixing. perhaps try add_proxy
        # apiResourceResultsIdTargets.add_method( # Adds a `Method` object
        #     "GET",
        #     target=api_.Integration( 
        #         type=api_.IntegrationType.AWS,
        #         options=api_.IntegrationOptions(
        #             passthrough_behavior=api_.PassthroughBehavior.WHEN_NO_TEMPLATES,
        #             request_templates={
        #                 'application/json' : """
        #                     {
        #                         "TableName": "${TargetsTable}",
        #                         "KeyConditionExpression": "JobID = :v1",
        #                         "ExpressionAttributeValues": {
        #                             ":v1": {
        #                                 "S": "$input.params('jobid')"
        #                             }
        #                         }
        #                     }
        #                 """
        #             },
        #             integration_responses=[
        #                 api_.IntegrationResponse(
        #                     status_code='200',
        #                     response_templates={
        #                             'application/json' : """#set($allTargs = $input.path('$.Items'))
        #                         {
        #                         "recordsTotal": $allTargs.size(),
        #                         "data" : [
        #                         #foreach($targ in $allTargs)
        #                         {
        #                             "Sequence": "$targ.Sequence.S",
        #                             "Start": "$targ.Start.N",
        #                             "End": "$targ.End.N",
        #                             "Strand": "$targ.Strand.S",
        #                             "Consensus": "$targ.Consensus.S",
        #                             "IsslScore": "$targ.IsslScore.S"
        #                         }#if($foreach.hasNext),#end
        #                         #end
        #                         ]
        #                         }"""
        #                     },
        #                     response_parameters={
        #                         'method.response.header.Access-Control-Allow-Headers' : 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        #                         'method.response.header.Access-Control-Allow-Methods' : 'POST,OPTIONS',
        #                         'method.response.header.Access-Control-Allow-Origin'  : '*'
        #                     },
        #                 )
        #             ]
        #         )
        #     ),
        #     method_responses=[
        #         api_.MethodResponse(
        #             response_models={
        #                 'application/json' : ''
        #             },
        #             response_parameters={
        #                 'method.response.header.Access-Control-Allow-Headers': 'true',
        #                 'method.response.header.Access-Control-Allow-Methods': 'true',
        #                 'method.response.header.Access-Control-Allow-Origin': 'true'
        #             },
        #             status_code='200'
        #         )
        #     ]
        # )

        # /submit
        apiResourceSubmitJob = apiRest.root.add_resource("submit") # returns an `IResource`
        apiResourceSubmitJob.add_method( # Adds a `Method` object
            "POST",
            api_.LambdaIntegration(lambdaCreateJob)
        )


app = cdk.App()
CracklingStack(app, "CracklingStackProd")
app.synth()