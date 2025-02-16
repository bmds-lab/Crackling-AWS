import boto3, os, re, json

from decimal import Decimal
from boto3.dynamodb.conditions import Key

from common_funcs import *

TARGETS_TABLE = os.getenv('TARGETS_TABLE')
JOBS_TABLE = os.getenv('JOBS_TABLE')
TASK_TRACKING_TABLE = os.getenv('TASK_TRACKING_TABLE')
CONSENSUS_SQS = os.getenv('CONSENSUS_QUEUE')
ISSL_SQS = os.getenv('ISSL_QUEUE')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TARGETS_TABLE)
sqsClient = boto3.client('sqs')

# Function that returns the reverse-complement of a given sequence
complements = str.maketrans('acgtrymkbdhvACGTRYMKBDHV', 'tgcayrkmvhdbTGCAYRKMVHDB')
def rc(dna):
    rcseq = dna.translate(complements)[::-1]
    return rcseq

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
                    'strand'    : strand,
                    'IsslScore' : None,
                    'Consensus' : None
                } 
    
    for possibleTarget in possibleTargets:
        if possibleTargets[possibleTarget]['count'] != 1:
            continue
        yield possibleTargets[possibleTarget]


# Find target sites and add to dictionary, 'candidateTargets'.
def find_targets(params):
    num_targets = 0

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

            num_targets += 1

    return num_targets


def deleteCandidateTargets(jobid):
    index = 0
    target_count = table.query(
        Select = 'COUNT',
        KeyConditionExpression = Key('JobID').eq(jobid)
    )['Count']
    
    with table.batch_writer() as batch:
        for i in range(0, target_count):
            batch.delete_item(Key={'JobID': jobid, 'TargetID': index})
            index += 1


def lambda_handler(event, context):
    # As this Lambda function is triggered by DynamoDB, we need to handle two
    # events: insertions and deletions.
    # See: https://docs.aws.amazon.com/lambda/latest/dg/with-ddb.html

    params, body = recv(event)
    
    accession = params['Genome']
    sequence = params['Sequence']
    jobId = params['JobID']
    
    num_targets = find_targets(params) 

    # set the total number of tasks the job needs to complete
    job = set_task_total(dynamodb, TASK_TRACKING_TABLE, jobId, num_targets)

    return None