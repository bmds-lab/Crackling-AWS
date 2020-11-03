import boto3, json, uuid, os, time

MAX_SEQ_LENGTH = os.getenv('MAX_SEQ_LENGTH', 10000)
JOBS_TABLE = os.getenv('JOBS_TABLE', 'jobs')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(JOBS_TABLE)

headers = {'Access-Control-Allow-Origin': '*'}


def return_400(message, tags = []):
    payload = {'message': message}
    if tags:
        payload['keys'] = tags
    body = json.dumps(payload)
    return {'statusCode': 400, 'headers': headers, 'body': body}

def write_job(jobid, sequence, jobtime):
    table.put_item(
        Item={
            'JobID': jobid,
            'Sequence': sequence,
            'DateTime': jobtime
        }
    )
    
def lambda_handler(event, context):
    if event['body']:
        try:
            job_request = json.loads(event['body'])
        except:
            return return_400('Error parsing request body. Is it properly formatted JSON?')
    else:
        return return_400('No body sent with request')

    if 'sequence' in job_request:
        sequence = job_request['sequence'].replace('\r\n', '').replace('\r', '').replace('\n', '').replace(' ', '')
        if len(sequence) == 0:
            return return_400('If specified, sequence must not be empty.', ['sequence'])
        elif len(sequence) > int(MAX_SEQ_LENGTH):
            return return_400(f'The specified sequence is too long (max length = {MAX_SEQ_LENGTH})', ['sequence'])

    jobid = str(uuid.uuid4())
    jobtime = int(time.time())
    write_job(jobid, sequence, jobtime)
    
    body = json.dumps({
        'aws_request_id' : context.aws_request_id,
        'JobID' : jobid
    })

    return {
        "statusCode": 200,
        "headers": headers,
        "body": body
    }
