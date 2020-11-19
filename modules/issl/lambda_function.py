import json
import boto3
import os
from subprocess import call
import tempfile

call(f"cp -r /opt/isslScoreOfftargets /tmp/isslScoreOfftargets".split(' '))
call(f"chmod -R 755 /tmp/isslScoreOfftargets".split(' '))
BIN_ISSL_SCORER = r"/tmp/isslScoreOfftargets"

OFFTARGETS_INDEX_MAP = {
    'Test100000_E_coli_offTargets_20.fa.sorted.issl' : r"/opt/Test100000_E_coli_offTargets_20.fa.sorted.issl",
    'Test200000_E_coli_offTargets_20.fa.sorted.issl' : r"/opt/Test200000_E_coli_offTargets_20.fa.sorted.issl"
}

targets_table_name = os.getenv('TARGETS_TABLE', 'TargetsTable')
jobs_table_name = os.getenv('JOBS_TABLE', 'JobsTable')

dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

TARGETS_TABLE = dynamodb.Table(targets_table_name)
JOBS_TABLE = dynamodb.Table(jobs_table_name)

def caller(*args, **kwargs):
    print(f"Calling: {args}")
    call(*args, **kwargs)
    
def CalcIssl(targets, genome):
    tmpToScore = tempfile.NamedTemporaryFile('w', delete=False)
    tmpScored = tempfile.NamedTemporaryFile('w', delete=False)

    # write the candidate guides to file
    with open(tmpToScore.name, 'w+') as fp:
        fp.write("\n".join([targets[x]['Seq20'] for x in targets]))
        fp.write("\n")

    # call the scoring method
    caller(
        ["{} \"{}\" \"{}\" \"{}\" \"{}\" > \"{}\"".format(
            BIN_ISSL_SCORER,
            OFFTARGETS_INDEX_MAP[genome],
            tmpToScore.name,
            '4',
            '75',
            tmpScored.name,
        )],
        shell = True
    )

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
    for message in event:
    
        #message = None
        #
        #try:
        #    if 'dynamodb' in record:
        #        if 'NewImage' in record['dynamodb']:
        #            message = record['dynamodb']['NewImage']
        #except Exception as e:
        #    print(f"Exception: {e}")
        #    continue
        #
        if not all([x in message for x in ['Sequence', 'JobID', 'TargetID']]):
            print(f'Missing core data to perform off-target scoring: {message}')
            continue
            
        print(message)
        
        # Transform the nested dict structure from Boto3 into something more
        # ideal, imo.
        # e.g. {
        #   'Count': {'N': '1'}, 
        #   'Sequence': {'S': 'ATCGATCGATCGATCGATCGAGG'}, 
        #   'JobID': {'S': '28653200-2afb-4d19-8369-545ff606f6f1'}, 
        #   'TargetID': {'N': '0'}
        # }
        t = {'S' : str, 'N' : int} # transforms
        f = {'Count' : 'N', 'Sequence' : 'S', 'JobID' : 'S', 'TargetID' : 'N'} # fields
        temp = {k : t[f[k]](message[k][f[k]]) for k in f}
        message = temp
        
                    
        jobId = message['JobID']
            
        if jobId not in jobToGenomeMap:
            print(f"JobID {jobId} not in job -> genome map.")
            # Fetch the job information so it is known which genome to use
            result = dynamodb_client.get_item(
                TableName = jobs_table_name,
                Key = {
                    'JobID' : {'S' : jobId}
                }
            )
            print(result)
            if 'Item' in result:
                genome = result['Item']['Genome']['S']
                
                jobToGenomeMap[jobId] = genome
                targetsToScorePerGenome[genome] = {}
                
                print(jobId, genome)
            else:
                print(f'No matching JobID: {jobId}???')
        else:  
            print(f"JobID {jobId} already in job -> genome map.")
          
        # key: genome, value: list of guides
        seq20 = message['Sequence'][0:20]
        targetsToScorePerGenome[jobToGenomeMap[jobId]][seq20] = {
            'JobID'     : jobId,
            'TargetID'  : message['TargetID'],
            'Seq'       : message['Sequence'],
            'Seq20'     : seq20,
            'Score'     : None,
        }

    print(f"Scoring guides on {len(targetsToScorePerGenome)} genome(s). Number of guides for each genome: ")
    print([len(targetsToScorePerGenome[x]) for x in targetsToScorePerGenome])
    
    for genome in targetsToScorePerGenome:
        # key: genome, value: list of dict
        targetsScored = CalcIssl(targetsToScorePerGenome[genome], genome)
    
        # now update the database with scores
        for key in targetsScored:
            result = targetsScored[key]
            print(f"Updating Job '{result['JobID']}'; Guide #{result['TargetID']}")
            response = TARGETS_TABLE.update_item(
                Key={'JobID': result['JobID'], 'TargetID': result['TargetID']},
                UpdateExpression='set IsslScore = :score',
                ExpressionAttributeValues={':score': json.dumps(result['Score'])}
            )
  
    return (event)
    