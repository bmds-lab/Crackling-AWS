import ast, json, os, re, tempfile
from subprocess import call
from time import time_ns

import boto3
from common_funcs import *

s3_bucket = os.environ['BUCKET']

def file_exists_in_s3(bucket_name, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

with open('/opt/sgrnascorer2_svm_data.json', 'r') as f:
    data = json.load(f)
support_vectors = data['support_vectors']
dual_coef = data['dual_coef']      
intercept = data['intercept']      
classes = data['classes']  


call(f"cp -r /opt/rnaFold /tmp/rnaFold".split(' '))
call(f"chmod -R 755 /tmp/rnaFold".split(' '))
BIN_RNAFOLD = r"/tmp/rnaFold/RNAfold"

low_energy_threshold = -30
high_energy_threshold = -18

targets_table_name = os.getenv('TARGETS_TABLE', 'TargetsTable')
task_tracking_table_name = os.getenv('TASK_TRACKING_TABLE')
consensus_queue_url = os.getenv('CONSENSUS_QUEUE', 'ConsensusQueue')

sqs_client = boto3.client('sqs')

dynamodb = boto3.resource('dynamodb')
TARGETS_TABLE = dynamodb.Table(targets_table_name)

def caller(*args, **kwargs):
    print(f"Calling: {args}")
    call(*args, **kwargs)
    
# Function that replaces U with T in the sequence (to go back from RNA to DNA)
def transToDNA(rna):
    switch_UT = str.maketrans('U', 'T')
    dna = rna.translate(switch_UT)
    return dna


def CalcConsensus(recordsByJobID):
    for jobid in recordsByJobID.keys():
        rnaFoldResults = _CalcRnaFold(recordsByJobID[jobid].keys())
        for record in recordsByJobID[jobid]:
            recordsByJobID[jobid][record]['Consensus'] = ','.join([str(int(x)) for x in [
                _CalcChopchop(record),
                _CalcMm10db(record, rnaFoldResults[record]['result']),
                _CalcSgrnascorer(record)
            ]])
    return recordsByJobID

def _CalcRnaFold(seqs):
    results = {} # as output

    guide = "GUUUUAGAGCUAGAAAUAGCAAGUUAAAAUAAGGCUAGUCCGUUAUCAACUUGAAAAAGUGGCACCGAGUCGGUGCUUUU"
    pattern_RNAstructure = r".{28}\({4}\.{4}\){4}\.{3}\){4}.{21}\({4}\.{4}\){4}\({7}\.{3}\){7}\.{3}\s\((.+)\)"
    pattern_RNAenergy = r"\s\((.+)\)"

    tmpToScore = tempfile.NamedTemporaryFile('w', delete=False)
    tmpScored = tempfile.NamedTemporaryFile('w', delete=False)

    with open(tmpToScore.name, 'w+') as fRnaInput:
        for seq in seqs:
            # we don't want guides on + starting with T, or on - ending with A
            # and only things that have passed everything else so far
            if  not ( 
                    (seq[-2:] == 'GG' and seq[0] == 'T') or 
                    (seq[:2] == 'CC' and seq[-1] == 'A')
                ):
                fRnaInput.write(
                    "G{}{}\n".format(
                        seq[1:20], 
                        guide
                    )
                )

    caller(
        "{} --noPS -j{} -i \"{}\" >> \"{}\"".format(
            BIN_RNAFOLD,
            4,
            tmpToScore.name,
            tmpScored.name
        ), 
        shell=True
    )

    total_number_structures = len(seqs)

    RNA_structures = None
    with open(tmpScored.name, 'r') as fRnaOutput:
        RNA_structures = fRnaOutput.readlines()

    i = 0
    for seq in seqs:
        results[seq] = {}
        results[seq]['result'] = 0
        
        # we don't want guides on + starting with T, or on - ending with A
        # and only things that have passed everything else so far
        if not ( 
                (seq[-2:] == 'GG' and seq[0] == 'T') or 
                (seq[:2] == 'CC' and seq[-1] == 'A')
            ): 
        
            L1 = RNA_structures[2 * i].rstrip()
            L2 = RNA_structures[2 * i + 1].rstrip()
            
            structure = L2.split(" ")[0]
            energy = L2.split(" ")[1][1:-1]
            
            results[seq]['L1'] = L1
            results[seq]['structure'] = structure
            results[seq]['energy'] = energy
            
            target = L1[:20]
            if transToDNA(target) != seq[0:20] and transToDNA("C"+target[1:]) != seq[0:20] and transToDNA("A"+target[1:]) != seq[0:20]:
                print("Error? "+seq+"\t"+target)
                continue

            match_structure = re.search(pattern_RNAstructure, L2)
            if match_structure:
                energy = ast.literal_eval(match_structure.group(1))
                if energy < float(low_energy_threshold):
                    results[transToDNA(seq)]['result'] = 0 # reject due to this reason
                else:
                    results[seq]['result'] = 1 # accept due to this reason
            else:
                match_energy = re.search(pattern_RNAenergy, L2)
                if match_energy:
                    energy = ast.literal_eval(match_energy.group(1))
                    if energy <= float(high_energy_threshold):
                        results[transToDNA(seq)]['result'] = 0 # reject due to this reason
                    else:
                        results[seq]['result'] = 1 # accept due to this reason
            i += 1
    return results

    
def _CalcChopchop(seq):
    '''
    CHOPCHOP accepts guides with guanine in position 20
    '''
    return (seq[19] == 'G')
    
def _CalcMm10db(seq, rnaFoldResult):
    '''
    mm10db accepts guides that:
        - do not contain poly-thymine seqs (TTTT)
        - AT% between 20-65%
        - Secondary structure energy
    '''
    
    AT = sum([c in 'AT' for c in seq])/len(seq)
    
    return all([
        'TTTT' not in seq,
        (AT >= 0.20 and AT <= 0.65),
        rnaFoldResult
    ])


# Dot product between two vectors
def dot(u, v):
    return sum(ue * ve for ue, ve in zip(u, v))

# Decision function
def decision_function(x, support_vectors, dual_coef, intercept):
    total = 0.0
    for coef, sv in zip(dual_coef, support_vectors):
        total += coef * dot(sv, x)
    return total + intercept

# Predict
def predict(x):
    score = decision_function(x, support_vectors, dual_coef, intercept)
    return (classes[1] if score > 0 else classes[0], score)

# sgRNAScorer 2.0 calculation
def _CalcSgrnascorer(seq):
    encoding = {
        'A': '0001', 'C': '0010', 'T': '0100', 'G': '1000',
        'K': '1100', 'M': '0011', 'R': '1001', 'Y': '0110',
        'S': '1010', 'W': '0101', 'B': '1110', 'V': '1011',
        'H': '0111', 'D': '1101', 'N': '1111'
    }

    # Flattened binary encoding of the first 20 bases
    entryList = []
    for base in seq[:20]:
        entryList.extend(int(bit) for bit in encoding[base])

    _, score = predict(entryList)
    return float(score) >= 0


def lambda_handler(event, context):
    records = {}
    recordsByJobID = {}
    
    ReceiptHandles = []
    for record in event['Records']:
        genome = ""
        try:
            message = json.loads(record['body'])
            genome = json.loads(message['genome'])
            message = json.loads(message['default'])
        except:
            continue

        if not all([x in message for x in ['Sequence', 'JobID', 'TargetID']]):
            print(f'Missing core data to perform off-target scoring: {message}')
            continue
            
        if message['JobID'] not in recordsByJobID:
            recordsByJobID[message['JobID']] = {}
        
        recordsByJobID[message['JobID']][message['Sequence']] = {
          'JobID'         : message['JobID'],
          'TargetID'      : message['TargetID'],
          'Consensus'     : "",
        }
            
        ReceiptHandles.append(record['receiptHandle'])

    results = CalcConsensus(recordsByJobID)

    # track number of tasks completed for each job by counting instances of each jobID
    job_tasks = {}
    
    for jobid in results.keys():
        for result in results[jobid].values():
            #print(json.dumps(result['Consensus']))
            response = TARGETS_TABLE.update_item(
                Key={'JobID': result['JobID'], 'TargetID': result['TargetID']},
                UpdateExpression='set Consensus = :c',
                ExpressionAttributeValues={':c': result['Consensus']}
            )
        
            # increment task counter for each job
            if result['JobID'] not in job_tasks:
                # if job doesnt have an entry, create one
                job_tasks.update({result['JobID'] : 1})
            else:
                job_tasks[result['JobID']] += 1

    # remove messages from the SQS queue. Max 10 at a time.
    for i in range(0, len(ReceiptHandles), 10):
        toDelete = [ReceiptHandles[j] for j in range(i, min(len(ReceiptHandles), i+10))]
        response = sqs_client.delete_message_batch(
            QueueUrl=consensus_queue_url,
            Entries=[
                {
                    'Id': f"{time_ns()}",
                    'ReceiptHandle': delete
                }
                for delete in toDelete
            ]
        )

    # Update task counter for each job
    for jobID, task_count in job_tasks.items():
        job = update_task_counter(dynamodb, task_tracking_table_name, jobID, "NumScoredOntarget", task_count)

    return (event)
    