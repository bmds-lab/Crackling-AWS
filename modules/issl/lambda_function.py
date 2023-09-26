import json, boto3, os, re, shutil, tempfile, sys
from time import time_ns
from subprocess import call

from common_funcs import *

shutil.copy("/opt/isslScoreOfftargets", "/tmp/isslScoreOfftargets")
call(f"chmod -R 755 /tmp/isslScoreOfftargets".split(' '))
BIN_ISSL_SCORER = r"/tmp/isslScoreOfftargets"


#environment variables for aws service endpoints
targets_table_name = os.getenv('TARGETS_TABLE', 'TargetsTable')
jobs_table_name = os.getenv('JOBS_TABLE', 'JobsTable')
issl_queue_url = os.getenv('ISSL_QUEUE', 'IsslQueue')

#boto3 aws clients
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
sqs_client = boto3.client('sqs')

s3_bucket = os.environ['BUCKET']
s3_log_bucket = os.environ['LOG_BUCKET']
s3_client = boto3.client('s3')

TARGETS_TABLE = dynamodb.Table(targets_table_name)
JOBS_TABLE = dynamodb.Table(jobs_table_name)
EFS_MOUNT_PATH = os.environ['EFS_MOUNT_PATH']

def caller(*args, **kwargs):
    print(f"Calling: {args}")
    call(*args, **kwargs)
    
def store_log(context, genome, jobId):
    #log name based on request_id, a unique identifier
    output = 'offtarget/Issl_'+ context.aws_request_id[0:8]
    #store lambda id for future logging
    create_log(s3_client, s3_log_bucket, context, genome, jobId, output)

def efs_genome_dir(accession):

    efs_destination_path = f"{EFS_MOUNT_PATH}/{accession}/issl"
    expected_file = f"{accession}.issl"

    #graceful failure based on conditions
    if not os.path.exists(efs_destination_path):
        sys.exit('Failure - The EFS directory does not exist')

    if not file_exist(efs_destination_path, [expected_file]):
        sys.exit('Failure - The required issl file is missing')

    #exact location of issl file in efs
    return f"{efs_destination_path}/{expected_file}"


def CalcIssl(targets, genome):
    tmpToScore = tempfile.NamedTemporaryFile('w', delete=False)
    tmpScored = tempfile.NamedTemporaryFile('w', delete=False)

    # Create tempfile that consists of each target guide sequence for a specific genome
    with open(tmpToScore.name, 'w+') as fp:
        fp.write("\n".join([targets[x]['Seq20'] for x in targets]))
        fp.write("\n")

    # Extract directory from Elastic File Storage (EFS) where the specific genome matches an .issl file
    issl_file = efs_genome_dir(genome)

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

    # Extract the score in scored temp file and insert into dictionary structure to be returned
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
    
    # Create dictionary mapping jobid to genome for all messages in SQS batch
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
            
        jobId = message['JobID']

        if jobId not in jobToGenomeMap:
            # Fetch the job information so it is known which genome to use
            result = dynamodb_client.get_item(
                TableName = jobs_table_name,
                Key = {
                    'JobID' : {'S' : jobId}
                }
            )
            # Extract genome from fetched result
            if 'Item' in result:
                genome = result['Item']['Genome']['S']
                jobToGenomeMap[jobId] = genome
                targetsToScorePerGenome[genome] = {}
                print(jobId, genome)

                # Benchmark code
                store_log(context, genome, jobId)
            
            # Error - Empty fetched result from dynamodb

            else:
                print(f'No matching JobID: {jobId}???')
                ReceiptHandles.append(record['receiptHandle'])
                continue

        # Extract target guide sequence (21-length long)
        seq20 = message['Sequence'][0:20]

        # Map guide sequences to genome and prepare dictionary structure for scoring in next iteration
        targetsToScorePerGenome[jobToGenomeMap[jobId]][seq20] = {
            'JobID'     : jobId,
            'TargetID'  : message['TargetID'],
            'Seq'       : message['Sequence'],
            'Seq20'     : seq20,
            'Score'     : None,
        }
        # Keep track of messages in batch for removal at later stage
        ReceiptHandles.append(record['receiptHandle'])
    #print(f"Scoring guides on {len(targetsToScorePerGenome)} genome(s). Number of guides for each genome: ", [len(targetsToScorePerGenome[x]) for x in targetsToScorePerGenome])
    
    # Next iteration - for each genome, score its target sequences
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

    # Update task counter for each job, and spawn a notification if a job is completed    
    job = update_task_counter(dynamodb, jobs_table_name, jobId, 1)

    #notify user if job is completed
    spawn_notification_if_complete(job,NOTIFICATION_SQS)

    
    return (event)