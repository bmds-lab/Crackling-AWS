import json
import boto3
import os,re
import shutil
from time import time_ns
from subprocess import call
import tempfile

from common_funcs import *

shutil.copy("/opt/isslScoreOfftargets", "/tmp/isslScoreOfftargets")
call(f"chmod -R 755 /tmp/isslScoreOfftargets".split(' '))
BIN_ISSL_SCORER = r"/tmp/isslScoreOfftargets"

targets_table_name = os.getenv('TARGETS_TABLE', 'TargetsTable')
jobs_table_name = os.getenv('JOBS_TABLE', 'JobsTable')
issl_queue_url = os.getenv('ISSL_QUEUE', 'IsslQueue')
s3_log_bucket = os.environ['LOG_BUCKET']

dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
sqs_client = boto3.client('sqs')

s3_log_client = boto3.client('s3')
TARGETS_TABLE = dynamodb.Table(targets_table_name)
JOBS_TABLE = dynamodb.Table(jobs_table_name)

def caller(*args, **kwargs):
    print(f"Calling: {args}")
    call(*args, **kwargs)
    
def CalcIssl(targets, genome):
    tmpToScore = tempfile.NamedTemporaryFile('w', delete=False)
    tmpScored = tempfile.NamedTemporaryFile('w', delete=False)

    # write the candidate guides to file
    with open(tmpToScore.name, 'w+') as fp:
        fp.write("\n".join([targets[x]['Seq20'] for x in targets]))
        fp.write("\n")

    # download from s3 based on accession
    s3_log_client = boto3.client('s3')
    s3_genome_client = boto3.client('s3', endpoint_url=genome_access_point_arnq)
    s3_bucket = os.environ['BUCKET']
    genome_access_point_arnq = os.environ['GENOME_ACCESS_POINT_ARN']
    
    _, issl_file = s3_files_to_tmp(s3_log_client,s3_bucket,genome,".issl")

    # call the scoring method
    caller(
        ["{} \"{}\" \"{}\" \"{}\" \"{}\" > \"{}\"".format(
            BIN_ISSL_SCORER,
            issl_file,
            tmpToScore.name,
            '4',
            '75',
            tmpScored.name,
        )],
        shell = True
    )

    with open(tmpScored.name, 'r') as fp:
        for targetScored in [x.split('\t') for x in fp.readlines()]:
            if len(targetScored) == 2:
                targets[targetScored[0]]['Score'] = float(targetScored[1].strip())

    return targets
    
def lambda_handler(event, context):
    # key: genome, value: list of guides
    targetsToScorePerGenome = {}
    
    # key: genome, value: list of dict
    targetsScored = {}
    
    # key: JobID, value: genome
    jobToGenomeMap = {}
    
    # score the targets in bulk first
    message = None
    
    # SQS receipt handles
    ReceiptHandles = []
    
    print(event)
    
    # SNS only pushes one message at time, so this for-loop is useless
    # tho still worthwhile in preparation for an architecture that can send
    # messages in batches (for example, via a DynamoDB stream)
    # but I'm out of time to finish that implemetation (there is a commit
    # in the repo where this is implemented, but functioning incorrectly [stuck
    # in a invocation loop])
    for record in event['Records']:
        try:
            message = json.loads(record['body'])
            message = json.loads(message['default'])
        except e:
            print(f"Exception: {e}")
            continue

        if not all([x in message for x in ['Sequence', 'JobID', 'TargetID']]):
            print(f'Missing core data to perform off-target scoring: {message}')
            continue
            
        # Transform the nested dict structure from Boto3 into something more
        # ideal, imo.
        # This is only needed if the payload is passed from a Lambda function.
        # SNS gives it to us in the "ideal" format already.
        # e.g. {
        #   'Count': {'N': '1'}, 
        #   'Sequence': {'S': 'ATCGATCGATCGATCGATCGAGG'}, 
        #   'JobID': {'S': '28653200-2afb-4d19-8369-545ff606f6f1'}, 
        #   'TargetID': {'N': '0'}
        # }
        #t = {'S' : str, 'N' : int} # transforms
        #f = {'Count' : 'N', 'Sequence' : 'S', 'JobID' : 'S', 'TargetID' : 'N'} # fields
        #temp = {k : t[f[k]](message[k][f[k]]) for k in f}
        #message = temp
 
        jobId = message['JobID']
            
        if jobId not in jobToGenomeMap:
            #print(f"JobID {jobId} not in job-to-genome map.")
            # Fetch the job information so it is known which genome to use
            result = dynamodb_client.get_item(
                TableName = jobs_table_name,
                Key = {
                    'JobID' : {'S' : jobId}
                }
            )
            
            
            
            if 'Item' in result:
                genome = result['Item']['Genome']['S']
                
                jobToGenomeMap[jobId] = genome
                targetsToScorePerGenome[genome] = {}
                
                print(jobId, genome)

                #log name based on request_id, a unique identifier
                output = 'offtarget/Issl_'+ context.aws_request_id[0:8]
                #store lambda id for future logging
                create_log(s3_log_client, s3_log_bucket, context, genome, result['Item']['Sequence']['S'], jobId, output)
                
            else:
                print(f'No matching JobID: {jobId}???')
                ReceiptHandles.append(record['receiptHandle'])
                continue

        # key: genome, value: list of guides
        seq20 = message['Sequence'][0:20]
        targetsToScorePerGenome[jobToGenomeMap[jobId]][seq20] = {
            'JobID'     : jobId,
            'TargetID'  : message['TargetID'],
            'Seq'       : message['Sequence'],
            'Seq20'     : seq20,
            'Score'     : None,
        }
        
        ReceiptHandles.append(record['receiptHandle'])

    #print(f"Scoring guides on {len(targetsToScorePerGenome)} genome(s). Number of guides for each genome: ", [len(targetsToScorePerGenome[x]) for x in targetsToScorePerGenome])
    
    for genome in targetsToScorePerGenome:
        # key: genome, value: list of dict
        targetsScored = CalcIssl(targetsToScorePerGenome[genome], genome)
    
        # now update the database with scores
        for key in targetsScored:
            result = targetsScored[key]
            #print({'JobID': result['JobID'], 'TargetID': result['TargetID'], 'key': key})
            response = TARGETS_TABLE.update_item(
                Key={'JobID': result['JobID'], 'TargetID': result['TargetID']},
                UpdateExpression='set IsslScore = :score',
                ExpressionAttributeValues={':score': json.dumps(result['Score'])},
                #ReturnValues='UPDATED_NEW'
            )
            #print(f"Updating Job '{result['JobID']}'; Guide #{result['TargetID']}; ", response['ResponseMetadata']['HTTPStatusCode'])
            #print(response)    
    
    # remove messages from the SQS queue. Max 10 at a time.
    for i in range(0, len(ReceiptHandles), 10):
        toDelete = [ReceiptHandles[j] for j in range(i, min(len(ReceiptHandles), i+10))]
        response = sqs_client.delete_message_batch(
            QueueUrl=issl_queue_url,
            Entries=[
                {
                    'Id': f"{time_ns()}",
                    'ReceiptHandle': delete
                }
                for delete in toDelete
            ]
        )
    
    return (event)