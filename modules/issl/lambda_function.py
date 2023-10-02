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
# HELPER
# ----------------------

def s3_to_tmp(tmp_dir, accession):
    download_info = {}
    expected_file = f"{accession}.issl"
    #store fasta file in lambda's local storage from s3
    tmp_dir_issl = f"{tmp_dir}/{expected_file}"
    s3_destination_path = f"{accession}/issl"
    s3_resource.meta.client.download_file(s3_bucket, f"{s3_destination_path}/{expected_file}", tmp_dir_issl)
    #recording path information
    download_info['ISSL_FILE_PATH'] = tmp_dir_issl
    return download_info

#Function creates dictionary to store Size and Length to make decision at future point
def getIsslInfo(issl_genomes, targetsToScorePerGenome):
    output = {}
    for genome in issl_genomes:
        info = {}
        key = f"{genome}/issl/{genome}.issl"
        info['Size'] = s3_get_file_size(s3_client, s3_bucket, key) / BYTE_TO_MB_DIVIDER
        info['Length'] = len(targetsToScorePerGenome[genome])
        output[genome] = info
    return output

#Function checks whether all issl files in batch can be downloaded into ephemeral storage
def canStoreAll(issl_dict):
    totalSize = 0
    for genome in issl_dict.keys():
        totalSize += issl_dict[genome]['Size']
    if (totalSize > MAX_EPHEMERAL_STORAGE_SIZE):
        return False
    return True

#returns the genome that fits "Length" (number of targets) criteria
def getOptimalChoice(keys, issl_dict):
    largestLength = 0
    largestLengthSize = 0
    largestLengthGenome = ""
    #algorithm to maximise best option based on len and genome size
    for genome in keys:
        genome_length = issl_dict[genome]['Length']
        genome_size = issl_dict[genome]['Size']
        #swap if previous genome had less targets
        if (largestLength < genome_length):
            #new largest values
            largestLength = genome_length
            largestLengthSize = genome_size
            largestLengthGenome = genome
        #the previous genome has same amount of targets
        elif (largestLength == genome_length):
            #swap if the matched genome has larger size
            if (largestLengthSize < genome_size):
                #new largest values
                largestLength = genome_length
                largestLengthSize = genome_size
                largestLengthGenome = genome
    
    return largestLengthGenome, largestLength, largestLengthSize

#returns list of genomes that best fit and do not over exceed local storage
def determineGenomesToKeep(issl_dict):
    memory_used = 0
    chosenGenomes = []
    keys = issl_dict.keys()
    keys_len = len(keys)
    count = 0
    while (memory_used <= MAX_EPHEMERAL_STORAGE_SIZE and count < keys_len):
        genome, _, size = getOptimalChoice(keys, issl_dict)
        expected_memory_used = memory_used + size 
        if (expected_memory_used < MAX_EPHEMERAL_STORAGE_SIZE):
            chosenGenomes.append(genome)
            memory_used = expected_memory_used
            issl_dict.pop(genome, "None")
        else:
            print(f"{genome} exceeds available storage. Added to removal list.")
            issl_dict.pop(genome, 'None')
        count += 1
    return chosenGenomes

def downloadFiles(tmp_dir, list_to_download):
    genomeDownloadInfo = {}
    for genome in list_to_download:
        genomeDownloadInfo[genome] = s3_to_tmp(tmp_dir, genome)
    return genomeDownloadInfo

def downloadIsslFiles(targetsToScorePerGenome, tmp_dir):
    #genomes in batch
    issl_genomes = list(targetsToScorePerGenome.keys())
    issl_len = len(issl_genomes)
    #empty batch
    if (issl_len <= 0):
        sys.exit('Failure - No targets required to score')
    #single genome batch
    elif (issl_len == 1):
        genome = issl_genomes[0]
        s3_destination_path = f"{genome}/issl"
        if not files_exist_s3_dir(s3_client, s3_bucket, s3_destination_path, [genome+".issl"]):
            sys.exit('Failure - The required issl file is missing')
        return downloadFiles(tmp_dir, [genome]), False
    #multi-genome batch
    else:
        if not issl_files_exist_s3(s3_client, s3_bucket, issl_genomes):
            sys.exit('Failure - The required issl files are missing')
        
        issl_dict = getIsslInfo(issl_genomes, targetsToScorePerGenome)

        if not canStoreAll(issl_dict):
            genomes_to_keep = determineGenomesToKeep(issl_dict)
            #download files that fit criteria
            return downloadFiles(tmp_dir, genomes_to_keep), True
        else:
            #download all files
            return downloadFiles(tmp_dir, issl_dict.keys()), False

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

    #targetsToScorePerGenome = {
    #'genome1': {'seq1': {}, 'seq2':{} },
    #'genome2': {'seq3': {}, 'seq4':{} } 
    # }
    #ReceiptHandles = ['dsdsdds', 'sdssd', 'sdsdsdsd']  
    #NEW-ReceiptHandles = {'GCA_004027125.1': ['dsdsdds', 'sdssd'], 'GCA_947508005.1': ['sdsdsdsd']}

    #get temp folder to download the issl files into
    tmp_dir = get_tmp_dir()
    #determine if local storage can download required issl files and remove unnecessary details if need be
    downloadInfo, skip_flag = downloadIsslFiles(targetsToScorePerGenome, tmp_dir)
    if (skip_flag):
        genomes_to_remove = [item for item in list(targetsToScorePerGenome.keys()) if item not in list(downloadInfo)]
        for accession in genomes_to_remove:
            targetsToScorePerGenome.pop(accession)
            ReceiptHandles.pop(accession)
    #translate dictionary into list of receipts
    ReceiptHandles = [item for row in list(ReceiptHandles.values()) for item in row]
    


    #Move receipts that are to be moved to DLQ
    #https://stackoverflow.com/questions/53807007/how-do-i-return-a-message-back-to-sqs-from-lambda-trigger
    #I can delete the receipts expected to be removed. The rest will utilise a visibility timeout 
    #to re-add into queue at a later date
    print(targetsToScorePerGenome)
    print(ReceiptHandles)

    #MAX TIME at 2 minutes currently
    # visibility timeout of 140 seconds seems appropriate



    #-------------------------------
    # SCORING
    #-------------------------------

    # Next iteration - for each genome, score its target sequences
    for genome in targetsToScorePerGenome:
        # key: genome, value: list of dict
        targetsScored = CalcIssl(targetsToScorePerGenome[genome], downloadInfo[genome]['ISSL_FILE_PATH'])
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