import boto3, os, re, json

from decimal import Decimal
from boto3.dynamodb.conditions import Key

TARGETS_TABLE = os.getenv('TARGETS_TABLE')
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
    with table.batch_writer() as batch:
        for index, target in enumerate(target_iterator(params['Sequence'])):
            targetEntry = create_target_entry(params, index, target)
            
            batch.put_item(Item=targetEntry)
            
            for targetQueue in [ISSL_SQS, CONSENSUS_SQS]:
                msg = json.dumps(
                    {
                        'default': json.dumps(targetEntry)
                    }
                )
            
                response = sqsClient.send_message(
                    QueueUrl=targetQueue,
                    MessageBody=msg,
                )
                
                #print(
                #    index, 
                #    target,
                #    targetQueue, 
                #    response['ResponseMetadata']['HTTPStatusCode'], 
                #    msg
                #)


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
    required = {'JobID': 'S', 'Sequence': 'S'}

    inserted = [r['dynamodb']['NewImage'] for r in event['Records'] if r['eventName'] == 'INSERT']
    for i in inserted:
        try:
            jobid = i['JobID']['S']
            params = {p: i[p][required[p]] for p in required}
            params['Sequence'] = clean_candidate_sequence(params['Sequence'])
        except:
            return 'Entry contains invalid information'
        find_targets(params)
        #print('Processed INSERT event for {}.'.format(jobid))
        
    removed = [r['dynamodb']['OldImage'] for r in event['Records'] if r['eventName'] == 'REMOVE']
    for r in removed:
        jobid = r['JobID']['S']
        deleteCandidateTargets(jobid)
        #print('Processed REMOVE event for {}.'.format(jobid))
    
    return None #'Completed {} tasks.'.format(len(inserted) + len(removed))
