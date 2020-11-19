import boto3, os, re, json

from decimal import Decimal
from boto3.dynamodb.conditions import Key

TARGETS_TABLE = os.getenv('TARGETS_TABLE')
CONSENSUS_LAMBDA = os.getenv('CONSENSUS_LAMBDA')
ISSL_LAMBDA = os.getenv('ISSL_LAMBDA')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TARGETS_TABLE)

lambdaClient = boto3.client('lambda')

def lambda_handler(event, context):

    results = []

    #print(event)
    inserts =[r['dynamodb']['NewImage'] for r in event['Records'] if r['eventName'] == 'INSERT']
    #print(inserts)
    for func in [CONSENSUS_LAMBDA, ISSL_LAMBDA]:
        print("Invoking: ", func, inserts)
        r = lambdaClient.invoke(
            FunctionName = func,
            InvocationType = 'Event',
            Payload = json.dumps(inserts)
        )
        
        results.append(r)
        
    return results
