import json
import boto3
import os
import tempfile
import glob
import joblib
import re
import ast
from time import time_ns

from sklearn.svm import SVC
from subprocess import call

from common_funcs import *

SGRNASCORER2_MODEL = joblib.load('/opt/model-py38-svc0232.txt')

call(f"cp -r /opt/rnaFold /tmp/rnaFold".split(' '))
call(f"chmod -R 755 /tmp/rnaFold".split(' '))
BIN_RNAFOLD = r"/tmp/rnaFold/RNAfold"
#os.chmod(BIN_RNAFOLD, 755)

low_energy_threshold = -30
high_energy_threshold = -18

targets_table_name = os.getenv('TARGETS_TABLE', 'TargetsTable')
task_tracking_table_name = os.getenv('TASK_TRACKING_TABLE')
consensus_queue_url = os.getenv('CONSENSUS_QUEUE', 'ConsensusQueue')
notification_queue_url = os.getenv('NOTIFICATION_QUEUE')

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
    
def _CalcSgrnascorer(seq):
    encoding = {
        'A' : '0001',        'C' : '0010',        'T' : '0100',        
        'G' : '1000',        'K' : '1100',        'M' : '0011',
        'R' : '1001',        'Y' : '0110',        'S' : '1010',        
        'W' : '0101',        'B' : '1110',        'V' : '1011',        
        'H' : '0111',        'D' : '1101',        'N' : '1111'
    }

    entryList = []

    x = 0
    while x < 20:
        y = 0
        while y < 4:
            entryList.append(int(encoding[seq[x]][y]))
            y += 1
        x += 1

    # predict based on the entry
    prediction = SGRNASCORER2_MODEL.predict([entryList])
    score = SGRNASCORER2_MODEL.decision_function([entryList])[0]

    return (float(score) >= 0)

def lambda_handler(event, context):
    records = {}
    recordsByJobID = {}
    
    ReceiptHandles = []
    print(event)
    for record in event['Records']:
        genome = ""
        #print(record)
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
       
    #print(f"Processing {len(records)} guides.")
    
    print(recordsByJobID)
    results = CalcConsensus(recordsByJobID)
    print(results)


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
        
            #print(f"Updating Job {result['JobID']}; Guide #{result['TargetID']}; ", response['ResponseMetadata']['HTTPStatusCode'])
        
    
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

    # Update task counter for each job, and spawn a notification if a job is completed    
    for jobID, task_count in job_tasks.items():
        job = update_task_counter(dynamodb, task_tracking_table_name, jobID, task_count)

        #notify user if job is completed
        spawn_notification_if_complete(dynamodb, task_tracking_table_name, job, notification_queue_url)

    return (event)
    