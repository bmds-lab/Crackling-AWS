import boto3, os, re, json

from decimal import Decimal
from boto3.dynamodb.conditions import Key

from common_funcs import *

TARGETS_TABLE = os.getenv('TARGETS_TABLE')
JOBS_TABLE = os.getenv('JOBS_TABLE')
TASK_TRACKING_TABLE = os.getenv('TASK_TRACKING_TABLE')
CONSENSUS_SQS = os.getenv('CONSENSUS_QUEUE')
NOTIFICATION_SQS = os.getenv('NOTIFICATION_QUEUE')
ISSL_SQS = os.getenv('ISSL_QUEUE')
s3_log_bucket = os.environ['LOG_BUCKET']

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TARGETS_TABLE)
sqsClient = boto3.client('sqs')

# Create S3 client
s3_client = boto3.client('s3')

# Function that returns the reverse-complement of a given sequence
complements = str.maketrans('acgtrymkbdhvACGTRYMKBDHV', 'tgcayrkmvhdbTGCAYRKMVHDB')
def rc(dna):
    rcseq = dna.translate(complements)[::-1]
    return rcseq
    
#Loads a FASTA file and creates a string <candidate_seq> from the sequence.
trans = str.maketrans('', '', '1234567890 \n')
def clean_candidate_sequence(rawsequence):
    sequence = str(rawsequence)
    return sequence.translate(trans).upper()


def create_target_entry(params, index, target):
    return {
        'JobID': params['JobID'],
        'TargetID': index,
        'Sequence': target['seq'],
        'Count': target['count'],
        'Start' : target['start'],
        'End' : target['end'],
        'Strand' : target['strand'],
    }

pattern_forward = r"(?=([ATCG]{21}GG))"
pattern_reverse = r"(?=(CC[ACGT]{21}))"
pattern_forward_compiled = re.compile(pattern_forward)
pattern_reverse_compiled = re.compile(pattern_reverse)

def target_iterator(seq):
    possibleTargets = {}


    # once for forward, once for reverse
    for p, strand, seqModifier in [
        [pattern_forward_compiled, '+', lambda x : x], 
        [pattern_reverse_compiled, '-', lambda x : rc(x)]
    ]:
        for m in p.finditer(seq):
            target23 = seqModifier(
                seq[m.start() : m.start() + 23]
            )
            if target23 in possibleTargets:
                possibleTargets[target23]['count'] += 1
            else:
                possibleTargets[target23] = {
                    'count'     : 1,
                    'start'     : m.start(),
                    'end'       : m.start() + 23,
                    'seq'       : target23,
                    'strand'    : strand
                } 
    
    for possibleTarget in possibleTargets:
        if possibleTargets[possibleTarget]['count'] != 1:
            continue
        yield possibleTargets[possibleTarget]


# Find target sites and add to dictionary, 'candidateTargets'.
def find_targets(params):
    taskCounter = 0

    with table.batch_writer() as batch:
        for index, target in enumerate(target_iterator(params['Sequence'])):
            targetEntry = create_target_entry(params, index, target)
            
            batch.put_item(Item=targetEntry)
            
            for targetQueue in [ISSL_SQS, CONSENSUS_SQS]:
                msg = json.dumps(
                    {
                        'default': json.dumps(targetEntry),
                        'genome': json.dumps(params['Genome'])
                    }
                )
            
                response = sqsClient.send_message(
                    QueueUrl=targetQueue,
                    MessageBody=msg,
                )

                taskCounter += 1 #increment task counter
                

                #print(
                #    index, 
                #    target,
                #    targetQueue, 
                #    response['ResponseMetadata']['HTTPStatusCode'], 
                #    msg
                #)
    return taskCounter


def deleteCandidateTargets(jobid):
    index = 0
    target_count = table.query(
        Select = 'COUNT',
        KeyConditionExpression = Key('JobID').eq(jobid)
    )['Count']
    
    with table.batch_writer() as batch:
        for i in range(0, target_count):
            #print(f"Deleting: ", {'JobID': jobid, 'TargetID': index})
            batch.delete_item(Key={'JobID': jobid, 'TargetID': index})
            index += 1


def lambda_handler(event, context):
    # As this Lambda function is triggered by DynamoDB, we need to handle two
    # events: insertions and deletions.
    # See: https://docs.aws.amazon.com/lambda/latest/dg/with-ddb.html
    #required = {'JobID': 'S', 'Genome': 'S', 'Chromosome': 'S', 'Location': 'N', 'Sequence': 'S'}
    # required = {'JobID': 'S', 'Sequence': 'S'}

    # inserted = [r['dynamodb']['NewImage'] for r in event['Records'] if r['eventName'] == 'INSERT']
    # for i in inserted:
    #     try:
    #         jobid = i['JobID']['S']
    #         params = {p: i[p][required[p]] for p in required}
    #         params['Sequence'] = clean_candidate_sequence(params['Sequence'])
    #     except:
    #         return 'Entry contains invalid information'


    params,body = recv(event)
    
    accession = params['Genome']
    sequence = params['Sequence']
    jobid = params['JobID']
    
    create_log(s3_client, s3_log_bucket, context, accession, jobid, 'TargetScan')
    
    taskCount = find_targets(params) 

    # set the total number of tasks the job needs to complete
    job = set_task_total(dynamodb, TASK_TRACKING_TABLE, jobid, taskCount)

    #just in case by some bizare circumstances target scan finishes after ISSL/Consensus, check if all jobs are completed
    spawn_notification_if_complete(job, NOTIFICATION_SQS)

        #print('Processed INSERT event for {}.'.format(jobid))
        
    # removed = [r['dynamodb']['OldImage'] for r in event['Records'] if r['eventName'] == 'REMOVE']
    # for r in removed:
    #     jobid = r['JobID']['S']
    #     deleteCandidateTargets(jobid)
        #print('Processed REMOVE event for {}.'.format(jobid))
    
    return None #'Completed {} tasks.'.format(len(inserted) + len(removed))