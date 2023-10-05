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

#Set max size of 9500MB to allow for scoring operations
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

def CalcIssl(targets, genome_issl_file):
    tmpToScore = tempfile.NamedTemporaryFile('w', delete=False)
    tmpScored = tempfile.NamedTemporaryFile('w', delete=False)

    # Create tempfile that consists of each target guide sequence for a specific genome
    with open(tmpToScore.name, 'w+') as fp:
        fp.write("\n".join([targets[x]['Seq20'] for x in targets]))
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

    # Extract the score in scored temp file and insert into dictionary structure to be returned
    with open(tmpScored.name, 'r') as fp:
        for targetScored in [x.split('\t') for x in fp.readlines()]:
            if len(targetScored) == 2:
                targets[targetScored[0]]['Score'] = float(targetScored[1].strip())
    return targets
    

#----------------------
# HELPER FUNCS
# ----------------------

#Function to store number of targets and .issl file size by genome
def getGenomeBatchData(targetsToScorePerGenome):
    output = {}
    for genome in targetsToScorePerGenome.keys():
        info = {}
        info['Length'] = len(targetsToScorePerGenome[genome])
        s3_source_path = f"{genome}/issl/{genome}.issl"
        info['Size'] = s3_get_file_size(s3_client, s3_bucket, s3_source_path) / BYTE_TO_MB_DIVIDER
        output[genome] = info
    return output

#Function to reduce by size of batch genome list and compare with max size
def canLambdaStore(issl_dict):
    totalSize = 0
    for genome in issl_dict.keys():
        totalSize += issl_dict[genome]['Size']
    if (totalSize > MAX_EPHEMERAL_STORAGE_SIZE):
        return False
    return True

#returns list of genomes that fit criteria and do not over exceed local storage
def determineGenomesToKeep(issl_dict):
    # sort dictionary by criteria: max length first, then by max size in case of match
    priorityListGenomes = sorted(issl_dict, key=lambda x: (issl_dict[x]["Length"], issl_dict[x]["Size"]), reverse=True)
    # keep track of memory to download
    memory_used = 0
    # output list constrained by total memory
    downloadListGenomes = []
    for genome in priorityListGenomes:
        total_memory = memory_used + issl_dict[genome]["Size"]
        if (total_memory < MAX_EPHEMERAL_STORAGE_SIZE):
            downloadListGenomes.append(genome)
            memory_used = total_memory
        else:
            print(f"{genome} exceeds available storage. To be excluded from download list")
    return downloadListGenomes

def s3_to_tmp(tmp_dir, accession):
    download_info = {}
    expected_file = f"{accession}.issl"

    #path locs
    s3_source_path = f"{accession}/issl/{expected_file}"
    tmp_download_path = f"{tmp_dir}/{expected_file}" 

    s3_resource.meta.client.download_file(s3_bucket, s3_source_path, tmp_download_path)
    
    download_info['ISSL_FILE_PATH'] = tmp_download_path
    return download_info

def sequentialGenomeDownload(tmp_dir, list_to_download):
    genomeDownloadInfo = {}
    for genome in list_to_download:
        genomeDownloadInfo[genome] = s3_to_tmp(tmp_dir, genome)
    return genomeDownloadInfo

def downloadIsslFiles(targetsToScorePerGenome, tmp_dir):
    genomes_in_batch = list(targetsToScorePerGenome.keys())
    #empty batch
    if (len(genomes_in_batch) <= 0):
        sys.exit('Failure - No targets required to score')
    if not issl_files_exist_s3(s3_client, s3_bucket, genomes_in_batch):
        sys.exit('Failure - The required issl files are missing')

    genomes_batch_info = getGenomeBatchData(targetsToScorePerGenome)
    #download genomes that fit criteria
    if not canLambdaStore(genomes_batch_info):
        genomes_to_download = determineGenomesToKeep(genomes_batch_info)
        return sequentialGenomeDownload(tmp_dir, genomes_to_download), True
    #download all genomes
    else:
        return sequentialGenomeDownload(tmp_dir, genomes_in_batch), False

#--------------
# MAIN
#--------------
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
    ReceiptHandles = {}
    
    print(event)

    #-----------------------------
    # ARRANGING DATA STRUCTURE 
    #-----------------------------
    
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
        ReceiptHandles[jobToGenomeMap[jobId]] = ReceiptHandles.get(jobToGenomeMap[jobId], [])
        ReceiptHandles[jobToGenomeMap[jobId]].append(record['receiptHandle'])
    #print(f"Scoring guides on {len(targetsToScorePerGenome)} genome(s). Number of guides for each genome: ", [len(targetsToScorePerGenome[x]) for x in targetsToScorePerGenome])
    
    #-----------------------------------
    #DETERMINING AVAILABLE SPACE
    #------------------------------------

    #get temp folder to download the issl files into
    tmp_dir = get_tmp_dir()
    #determine if local storage can download required issl files and remove unnecessary details if need be
    downloaded_genomes, skip_flag = downloadIsslFiles(targetsToScorePerGenome, tmp_dir)
    if (skip_flag):
        genomes_to_remove = [item for item in list(targetsToScorePerGenome.keys()) if item not in list(downloaded_genomes)]
        #remove from scoring and sqs deletion
        for accession in genomes_to_remove:
            targetsToScorePerGenome.pop(accession)
            ReceiptHandles.pop(accession)

    print(targetsToScorePerGenome)

    #dictionary into list
    ReceiptHandles = [item for row in list(ReceiptHandles.values()) for item in row]
    print(ReceiptHandles)

    #-------------------------------
    # SCORING
    #-------------------------------

    # Next iteration - for each genome, score its target sequences
    for genome in targetsToScorePerGenome:
        # key: genome, value: list of dict
        targetsScored = CalcIssl(targetsToScorePerGenome[genome], downloaded_genomes[genome]['ISSL_FILE_PATH'])
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
        
    #close temp issl file directory
    if os.path.exists(tmp_dir):
        print("Cleaning Up...")
        shutil.rmtree(tmp_dir)  

    #------------------------------
    # REMOVAL FROM QUEUE
    #------------------------------
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