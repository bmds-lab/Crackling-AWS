import boto3, os, re, json

from decimal import Decimal
from boto3.dynamodb.conditions import Key

TARGETS_TABLE = os.getenv('TARGETS_TABLE')
CONSENSUS_SNS = os.getenv('CONSENSUS_TOPIC')
ISSL_SNS = os.getenv('ISSL_TOPIC')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TARGETS_TABLE)
snsClient = boto3.client('sns')

# Function that returns the reverse-complement of a given sequence
def rc(dna):
    complements = str.maketrans('acgtrymkbdhvACGTRYMKBDHV', 'tgcayrkmvhdbTGCAYRKMVHDB')
    rcseq = dna.translate(complements)[::-1]
    return rcseq
    
#Loads a FASTA file and creates a string <candidate_seq> from the sequence.
def clean_candidate_sequence(rawsequence):
    sequence = str(rawsequence)
    trans = str.maketrans('', '', '1234567890 \n')
    return sequence.translate(trans).upper()


def create_target_entry(params, index, target):
    entry = {
        'JobID': params['JobID'],
        #'Chromosome': params['Chromosome'],
        #'Location': int(params['Location']) + target[1],
        'TargetID': index,
        'Sequence': target[0],
        'Count': target[1],
        #'Adjacent': target[4][0],
        #'Downstream': target[4],
        #'Upstream': target[2],
        #'Position': target[1],
        #'GCContent': gc_content(target[3]),
        #'Strand': target[0]
    }

    return entry


def target_iterator(seq):
    possibleTargets = {}

    pattern_forward = r"(?=([ATCG]{21}GG))"
    pattern_reverse = r"(?=(CC[ACGT]{21}))"

    # once for forward, once for reverse
    for pattern, seqModifier in [
        [pattern_forward, lambda x : x], 
        [pattern_reverse, lambda x : rc(x)]
    ]:
        match_seq = re.findall(pattern, seq)
        if match_seq:
            for i in range(0, len(match_seq)):
                target23 = seqModifier(match_seq[i])
                if target23 in possibleTargets:
                    possibleTargets[target23] += 1
                else:
                    possibleTargets[target23] = 1 
    
    for possibleTarget in possibleTargets:
        if possibleTargets[possibleTarget] != 1:
            continue
        yield (possibleTarget, possibleTargets[possibleTarget])


# Find target sites and add to dictionary, 'candidateTargets'.
def find_targets(params):
    with table.batch_writer() as batch:
        for index, target in enumerate(target_iterator(params['Sequence'])):
            targetEntry = create_target_entry(params, index, target)
            
            batch.put_item(Item=targetEntry)
            
            for targetTopic in [ISSL_SNS, CONSENSUS_SNS]:
                msg = json.dumps(
                    {
                        'default': json.dumps(targetEntry)
                    }
                )
            
                response = snsClient.publish(
                    TargetArn=targetTopic,
                    Message=msg,
                    MessageStructure='json'
                )
                
                print(
                    index, 
                    target,
                    targetTopic.split(':')[5], 
                    response['ResponseMetadata']['HTTPStatusCode'], 
                    msg
                )


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
        print('Processed INSERT event for {}.'.format(jobid))
        
    removed = [r['dynamodb']['OldImage'] for r in event['Records'] if r['eventName'] == 'REMOVE']
    for r in removed:
        jobid = r['JobID']['S']
        deleteCandidateTargets(jobid)
        print('Processed REMOVE event for {}.'.format(jobid))
    
    return 'Completed {} tasks.'.format(len(inserted) + len(removed))
