import json
import boto3
import os

targets_table_name = os.getenv('TARGETS_TABLE', 'TargetsTable')
dynamodb = boto3.resource('dynamodb')
TARGETS_TABLE = dynamodb.Table(targets_table_name)


def CalcIssl(message):
    return 0.0
    
def lambda_handler(event, context):
    print(event)
    
    for record in event['Records']:
    
        message = None
    
        try:
            if 'Sns' in record:
                if 'Message' in record['Sns']:
                    message = json.loads(record['Sns']['Message'])
        except e:
            continue

        isslScore = CalcIssl(message)
        
        response = TARGETS_TABLE.update_item(
            Key={'JobID': message['JobID'], 'TargetID': message['TargetID']},
            UpdateExpression='set IsslScore = :score',
            ExpressionAttributeValues={':score': json.dumps(isslScore)}
        )
        
        print(message['Sequence'], consensus, response)
        
    return (event)
    