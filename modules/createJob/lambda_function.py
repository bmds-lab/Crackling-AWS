import boto3, json, uuid, os, time


from time import time,time_ns, sleep
from datetime import datetime
from common_funcs import *


MAX_SEQ_LENGTH = os.getenv('MAX_SEQ_LENGTH', 10000)
JOBS_TABLE = os.getenv('JOBS_TABLE', 'jobs')

s3_log_bucket = os.environ['LOG_BUCKET']
s3_client = boto3.client('s3')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(JOBS_TABLE)

headers = {
    'Access-Control-Allow-Headers'  : 'Content-Type',
    'Access-Control-Allow-Origin'   : '*'
}

def return_http_json(code, message, tags = []):
    payload = {'message': message}
    if tags:
        payload['keys'] = tags
    body = json.dumps(payload)
    return {'statusCode': code, 'headers': headers, 'body': body}

def lambda_handler(event, context):
    if event['body']:
        try:
            job_request = json.loads(event['body'])
        except:
            return return_http_json('Error parsing request body. Is it properly formatted JSON?',400)
    else:
        return return_http_json('No body sent with request',400)

    if 'sequence' in job_request:
        sequence = job_request['sequence'].replace('\r\n', '').replace('\r', '').replace('\n', '').replace(' ', '')
        if len(sequence) == 0:
            return return_http_json(400, 'If specified, sequence must not be empty.', ['sequence'])
        elif len(sequence) > int(MAX_SEQ_LENGTH):
            return return_http_json(400, f'The specified sequence is too long (max length = {MAX_SEQ_LENGTH})', ['sequence'])

    genome = job_request['genome']

    jobid = str(uuid.uuid4())

    table.put_item(
        Item={
            'JobID' : jobid,
            'Sequence' : sequence,
            'DateTime' : int(time()),
            'DateTimeHuman' : str(datetime.now()),
            'Genome' : genome,
            'Email' : "mattias.winsen@outlook.com"
            'TotalTasks' : -1,
            'CompletedTasks' : 0,
            'Version': 0
        }
    )
    
    body = json.dumps({
        'aws_request_id' : context.aws_request_id,
        'JobID' : jobid,
        'Genome' : genome # for debug
    })

    #Store lambda id for logging purposes
    create_log(s3_client, s3_log_bucket, context, genome, jobid, 'CreateJob')
    
    return {
        "statusCode": 200,
        "headers": headers,
        "body": body
    }