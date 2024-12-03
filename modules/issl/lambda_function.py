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
task_tracking_table_name = os.getenv('TASK_TRACKING_TABLE')
issl_queue_url = os.getenv('ISSL_QUEUE', 'IsslQueue')

#boto3 aws clients
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
sqs_client = boto3.client('sqs')

s3_bucket = os.environ['BUCKET']
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

#Set max size of 9500MB to allow for scoring operations
MAX_EPHEMERAL_STORAGE_SIZE = 9500
#byte -> megabyte magnitude
BYTE_TO_MB_DIVIDER = 1048576

TARGETS_TABLE = dynamodb.Table(targets_table_name)
JOBS_TABLE = dynamodb.Table(jobs_table_name)

def caller(*args, **kwargs):
    print(f"Calling: {args}")
    call(*args, **kwargs)

def CalcIssl(targets, genome_issl_file):
    tmpToScore = tempfile.NamedTemporaryFile('w', delete=False)
    tmpScored = tempfile.NamedTemporaryFile('w', delete=False)

    ## ASSUMPTION: the order of guides to AND from ISSL is the same

    # Create a temporary file containing a list of candidate guides to score
    with open(tmpToScore.name, 'w+') as fp:
        fp.write("\n".join([target['Seq'][0:20] for target in targets]))
        fp.write("\n")

    # call the scoring method
    caller(
        ["{} \"{}\" \"{}\" \"{}\" \"{}\" > \"{}\"".format(
            BIN_ISSL_SCORER,
            genome_issl_file,
            tmpToScore.name,
            '4',
            '75',
            tmpScored.name,
        )],
        shell = True
    )

    # Extract the score and associate it with the candidate guide
    with open(tmpScored.name, 'r') as fp:
        lines = [x.split('\t') for x in fp.readlines()]
        for idx, targetScored in enumerate(lines):
            if len(targetScored) == 2:
                targets[idx]['Score'] = float(targetScored[1].strip())

    return targets
    

#----------------------
# HELPER FUNCS
# ----------------------

# Function to store number of targets and .issl file size by genome
def getGenomeBatchData(genomes):
    return {
        genome : s3_get_file_size(s3_client, s3_bucket, f"{genome}/issl/{genome}.issl") / BYTE_TO_MB_DIVIDER
        for genome in genomes
    }

#Function to reduce by size of batch genome list and compare with max size
def canLambdaStore(issl_dict):
    return sum(issl_dict.values()) > MAX_EPHEMERAL_STORAGE_SIZE

# returns list of genomes that fit criteria and do not over exceed local storage
def determine_genomes_to_download(issl_dict):
    priorityListGenomes = sorted(issl_dict, key=lambda x: issl_dict[x], reverse=True)
    memory_used = 0
    genomesToDownload = []

    for genome in priorityListGenomes:
        total_memory = memory_used + issl_dict[genome]

        if (total_memory < MAX_EPHEMERAL_STORAGE_SIZE):
            genomesToDownload.append(genome)
            memory_used = total_memory

    return genomesToDownload

def s3_to_tmp(tmp_dir, accession):
    fp = f"{tmp_dir}/{accession}.issl"

    s3_resource.meta.client.download_file(
        s3_bucket, 
        f"{accession}/issl/{accession}.issl", # file in S3
        fp # local filesystem
    )
    
    return fp

def sequentialGenomeDownload(tmp_dir, list_to_download):
    return {
        genome : s3_to_tmp(tmp_dir, genome)
        for genome in list_to_download
    }

def downloadIsslFiles(genomes, tmp_dir):
    if len(genomes) <= 0:
        print('Failure - No targets required to score')

    if not issl_files_exist_s3(s3_client, s3_bucket, genomes):
        print('Failure - The required issl files are missing')

    genomes_batch_info = getGenomeBatchData(genomes)

    if not canLambdaStore(genomes_batch_info):
        genomes_to_download = determine_genomes_to_download(genomes_batch_info)
        return sequentialGenomeDownload(tmp_dir, genomes_to_download), True

    else:
        return sequentialGenomeDownload(tmp_dir, genomes), False

def resendGenomeToSQS(entries):
    print("Sending back to sqs")
    for entry in entries:
        response = sqs_client.send_message(
            QueueUrl=issl_queue_url,
            MessageBody=json.dumps(entry),
        )
        print(response)

    
#--------------
# MAIN
#--------------
def lambda_handler(event, context):
    
    # key: genome, value: list of guides
    genomeToTargets = {}
    
    # key: genome, value: list of dict
    targetsScored = {}
    
    # key: JobID, value: genome
    jobToGenome = {}
    
    # key: JobId, value: number of targets scored
    jobToNumTargets = {}

    # score the targets in bulk first
    message = None
    
    # SQS receipt handles
    receiptHandles = {}
    
    # SQS message 
    receiptMessages = {}
    
    print(event)

    #-----------------------------
    # ARRANGING DATA STRUCTURE 
    #-----------------------------
    
    # Create dictionary mapping jobid to genome for all messages in SQS batch
    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            message = json.loads(body['default'])
        except Exception as e:
            print(f"Exception: {e}")
            continue

        if not all([x in message for x in ['Sequence', 'JobID', 'TargetID']]):
            print(f'Missing core data to perform off-target scoring: {message}')
            continue
            
        jobId = message['JobID']

        if jobId not in jobToNumTargets:
            jobToNumTargets[jobId] = 0

        if jobId not in jobToGenome:
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
                jobToGenome[jobId] = genome
                genomeToTargets[genome] = []
                receiptMessages[genome] = []
            
            # Error - Empty fetched result from dynamodb
            else:
                print(f'No matching JobID: {jobId}???')
                receiptHandles.append(record['receiptHandle'])
                continue
        else:
            genome = jobToGenome[jobId]

        # Map guide sequences to genome and prepare dictionary structure for scoring in next iteration
        genomeToTargets[genome].append({
            'JobID'     : jobId,
            'TargetID'  : message['TargetID'],
            'Seq'       : message['Sequence'],
            'Score'     : None
        })
        
        #keep track of message sent by target scan function in case of resending required
        receiptMessages[genome].append(body)
        
        receiptHandles.setdefault(genome, []).append(record['receiptHandle'])

    #-----------------------------------
    # DETERMINING AVAILABLE SPACE
    #------------------------------------

    # get temp folder to download the issl files into
    tmp_dir = get_tmp_dir()

    # determine if local storage can download required issl files and remove unnecessary details if need be
    downloaded_genomes, skip_flag = downloadIsslFiles(list(genomeToTargets.keys()), tmp_dir)
    if (skip_flag):

        genomes_to_remove = [genome for genome in genomeToTargets if genome not in downloaded_genomes]

        for genome in genomes_to_remove:
            genomeToTargets.pop(genome)
            receiptHandles.pop(genome)

            resendGenomeToSQS(receiptMessages.pop(genome))

    receiptHandles = [item for row in list(receiptHandles.values()) for item in row]

    #-------------------------------
    # SCORING
    #-------------------------------

    # Next iteration - for each genome, score its target sequences
    for genome in genomeToTargets:
        targetsScored = CalcIssl(genomeToTargets[genome], downloaded_genomes[genome])

        for result in targetsScored:

            TARGETS_TABLE.update_item(
                Key={'JobID': result['JobID'], 'TargetID': result['TargetID']},
                UpdateExpression='set IsslScore = :score',
                ExpressionAttributeValues={':score': json.dumps(result['Score'])}
            )

            #jobToNumTargets[result['JobID']] += 1
            update_task_counter(dynamodb, task_tracking_table_name, result['JobID'], "NumScoredOfftarget", 1)

    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)  

    #------------------------------
    # REMOVAL FROM QUEUE
    #------------------------------
    # remove messages from the SQS queue. Max 10 at a time.

    for i in range(0, len(receiptHandles), 10):
        toDelete = [receiptHandles[j] for j in range(i, min(len(receiptHandles), i+10))]
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

    # Update task counter for each job
    #for jobId in jobToNumTargets:
    #    update_task_counter(dynamodb, task_tracking_table_name, jobId, "NumScoredOfftarget", jobToNumTargets[jobId])
