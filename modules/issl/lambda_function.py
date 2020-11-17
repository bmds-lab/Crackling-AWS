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
    
    targetsScored = {}
    with open(tmpScored.name, 'r') as fp:
        for targetScored in [x.split('\t') for x in fp.readlines()]:
            if len(targetScored) == 2:
                targetsScored[targetScored[0]] = float(targetScored[1].strip())

    return targetsScored
    
def lambda_handler(event, context):
    targetsToScore = []
    targetsScored = []
    
    # score the targets in bulk first
    for record in event['Records']:
    
        message = None
    
        try:
            if 'Sns' in record:
                if 'Message' in record['Sns']:
                    message = json.loads(record['Sns']['Message'])
        except e:
            continue
            
        targetsToScore.append(message['Sequence'])
            
            
    targetsScored = CalcIssl(targetsToScore)
    
    # now update the database with scores
    for record in event['Records']:
        response = TARGETS_TABLE.update_item(
            Key={'JobID': message['JobID'], 'TargetID': message['TargetID']},
            UpdateExpression='set IsslScore = :score',
            ExpressionAttributeValues={':score': json.dumps(targetsScored[message['Sequence'][0:20]])}
        )
  
    return (event)
    