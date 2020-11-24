import boto3, json, uuid, os, time, datetime

MAX_SEQ_LENGTH = os.getenv('MAX_SEQ_LENGTH', 10000)
JOBS_TABLE = os.getenv('JOBS_TABLE', 'jobs')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(JOBS_TABLE)

headers = {
    'Access-Control-Allow-Headers'  : 'Content-Type',
    'Access-Control-Allow-Origin'   : '*'
}

GENOMES_MAP = {
    'SARS_COV_2' : 'SARS-COV-2_NC_045512-2.issl'
    #'1' : 'Test100000_E_coli_offTargets_20.fa.sorted.issl',
    #'2' : 'Test200000_E_coli_offTargets_20.fa.sorted.issl',
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
            return return_http_json('Error parsing request body. Is it properly formatted JSON?')
    else:
        return return_http_json('No body sent with request')

    if 'sequence' in job_request:
        sequence = job_request['sequence'].replace('\r\n', '').replace('\r', '').replace('\n', '').replace(' ', '')
        if len(sequence) == 0:
            return return_http_json(400, 'If specified, sequence must not be empty.', ['sequence'])
        elif len(sequence) > int(MAX_SEQ_LENGTH):
            return return_http_json(400, f'The specified sequence is too long (max length = {MAX_SEQ_LENGTH})', ['sequence'])

    if 'genome' in job_request:
        if job_request['genome'] in GENOMES_MAP:
            genome = GENOMES_MAP[job_request['genome']]
        else:
            return return_http_json(400, 'Invalid genome selected')
    else:
        return return_http_json(400, 'No genome selected')

    jobid = str(uuid.uuid4())
    #jobid = str(int(time.time()))

    table.put_item(
        Item={
            'JobID' : jobid,
            'Sequence' : sequence,
            'DateTime' : int(time.time()),
            'DateTimeHuman' : str(datetime.datetime.now()),
            'Genome' : genome
        }
    )
    
    body = json.dumps({
        'aws_request_id' : context.aws_request_id,
        'JobID' : jobid
    })

    return {
        "statusCode": 200,
        "headers": headers,
        "body": body
    }