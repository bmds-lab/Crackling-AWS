import json
import boto3
import os
from subprocess import call
import tempfile

call(f"cp -r /opt/isslScoreOfftargets /tmp/isslScoreOfftargets".split(' '))
call(f"chmod -R 755 /tmp/isslScoreOfftargets".split(' '))
BIN_ISSL_SCORER = r"/tmp/isslScoreOfftargets"

OFFTARGETS_INDEX = r"/opt/Test200000_E_coli_offTargets_20.fa.sorted.issl"

targets_table_name = os.getenv('TARGETS_TABLE', 'TargetsTable')
dynamodb = boto3.resource('dynamodb')
TARGETS_TABLE = dynamodb.Table(targets_table_name)


def caller(*args, **kwargs):
    print(f"Calling: {args}")
    call(*args, **kwargs)
    
def CalcIssl(targets):
    tmpToScore = tempfile.NamedTemporaryFile('w', delete=False)
    tmpScored = tempfile.NamedTemporaryFile('w', delete=False)

    # write the candidate guides to file
    with open(tmpToScore.name, 'w+') as fp:
        fp.write("\n".join([x[0:20] for x in targets]))
        fp.write("\n")

    # call the scoring method
    caller(
        ["{} \"{}\" \"{}\" \"{}\" \"{}\" > \"{}\"".format(
            BIN_ISSL_SCORER,
            OFFTARGETS_INDEX,
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
    targetsToScore = {}
    targetsScored = {}
    
    # score the targets in bulk first
    for record in event['Records']:
    
        message = None
        
        try:
            if 'dynamodb' in record:
                if 'NewImage' in record['dynamodb']:
                    message = record['dynamodb']['NewImage']
        except Exception as e:
            print(f"Exception: {e}")
            continue
        
        if not all([x in message for x in ['Sequence', 'JobID', 'TargetID']]):
            print(f'Missing core data to perform consensus: {message}')
            continue
          
        # e.g. {
        #   'Count': {'N': '1'}, 
        #   'Sequence': {'S': 'ATCGATCGATCGATCGATCGAGG'}, 
        #   'JobID': {'S': '28653200-2afb-4d19-8369-545ff606f6f1'}, 
        #   'TargetID': {'N': '0'}
        # }
        t = {'S' : str, 'N' : int} # transforms
        f = {'Count' : 'N', 'Sequence' : 'S', 'JobID' : 'S', 'TargetID' : 'N'} # fields
        messageNew = {k : t[f[k]](message[k][f[k]]) for k in f}

        message = messageNew
          
        targetsToScore[message['Sequence'][0:20]] = {
            'JobID'     : message['JobID'],
            'TargetID'  : message['TargetID'],
            'Seq'       : message['Sequence'],
            'Score'     : None,
        }
            
   
    print(f"Scoring {len(targetsToScore)} guides.")  
    
    targetsScored = CalcIssl(targetsToScore)
    
    # now update the database with scores
    for key in targetsScored:
        result = targetsScored[key]
        print(f"Updating table for guide #{result['TargetID']}")
        response = TARGETS_TABLE.update_item(
            Key={'JobID': result['JobID'], 'TargetID': result['TargetID']},
            UpdateExpression='set IsslScore = :score',
            ExpressionAttributeValues={':score': json.dumps(result['Score'])}
        )
  
    return (event)
    