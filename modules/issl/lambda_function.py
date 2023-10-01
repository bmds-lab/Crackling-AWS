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
s3_resource = boto3.resource('s3')

#The max size is 9500MB
MAX_EPHEMERAL_STORAGE_SIZE = 9500
#byte -> megabyte magnitude
BYTE_TO_MB_DIVIDER = 1048576

TARGETS_TABLE = dynamodb.Table(targets_table_name)
JOBS_TABLE = dynamodb.Table(jobs_table_name)

def caller(*args, **kwargs):
    print(f"Calling: {args}")
    call(*args, **kwargs)
    
def store_log(context, genome, jobId):
    #log name based on request_id, a unique identifier
    output = 'offtarget/Issl_'+ context.aws_request_id[0:8]
    #store lambda id for future logging
    create_log(s3_client, s3_log_bucket, context, genome, jobId, output)

def s3_to_tmp_dir(accession):
    tmp_dir = get_tmp_dir()
    s3_destination_path = f"{accession}/issl"
    expected_file = f"{accession}.issl"
    if not files_exist_s3_dir(s3_client, s3_bucket, s3_destination_path, [expected_file]):
        sys.exit('Failure - The required issl file is missing')
    tmp_dir_fasta = f"{tmp_dir}/{expected_file}"
    #store fasta file in lambda's local storage
    s3_resource.meta.client.download_file(s3_bucket, f"{s3_destination_path}/{expected_file}", tmp_dir_fasta)
    return tmp_dir_fasta, tmp_dir

# def canIsslDownloadAll(issl_sizes):
#     totalSize = 0
#     for size in issl_sizes:
#         totalSize += size
#     totalSizeInMB = totalSize/BYTE_TO_MB_DIVIDER
#     print(totalSizeInMB)
#     if (totalSizeInMB > MAX_EPHEMERAL_STORAGE_SIZE):
#         return False
#     return True

# def getIsslSizes(accession_list):
#     isslSizes = {}
#     for accession in accession_list:
#         file_to_expect = f"{accession}.issl"
#         key = f"{accession}/issl/{file_to_expect}"
#         isslSizes[accession] = s3_get_file_size(s3_client, s3_bucket, key)
#     return isslSizes

def CalcIssl(targets, genome):
    tmpToScore = tempfile.NamedTemporaryFile('w', delete=False)
    tmpScored = tempfile.NamedTemporaryFile('w', delete=False)

    # Create tempfile that consists of each target guide sequence for a specific genome
    with open(tmpToScore.name, 'w+') as fp:
        fp.write("\n".join([targets[x]['Seq20'] for x in targets]))
        fp.write("\n")

    # Extract directory from Elastic File Storage (EFS) where the specific genome matches an .issl file
    issl_file, _ = s3_to_tmp_dir(genome)

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
    

    #TESTING DATA STRUCTURES - to be removed
    print(targetsToScorePerGenome)
    print(targetsScored)
    print(jobToGenomeMap)
    print(message)
    print(ReceiptHandles)

    #determine if local storage can download required issl files

    # #list of genomes acquired from batch
    # issl_genomes = list(targetToScorePerGenome.keys())
    # print(issl_genomes)
    # if not issl_files_exist_s3(s3_client, s3_bucket, issl_genomes)
    #     sys.exit('Failure - The required issl files are missing')
    # #dictionary mapping existing and required genome accessions to their size
    # issl_dict = getIsslSizes(issl_genomes)
    # print(issl_dict)
    # if not canIsslDownloadAll(list(issl_dict.values())):
    #     output, output_to_DQL = determineBestChoice(issl_dict)
    #     #update the targetToScorePerGenome dictionary to reflect changes
    #     targetToScorePerGenome = output
    #     #update receipts to reflect changes

    # #get temp folder to download the issl files into
    # tmp_dir = get_tmp_dir()
    # #download all required genomes
    # s3_issl_file_to_tmp(issl_genomes, tmp_dir)

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
    
    #close temp fasta file directory
    # if os.path.exists(tmp_dir):
    #     print("Cleaning Up...")
    #     shutil.rmtree(tmp_dir)

    return (event)