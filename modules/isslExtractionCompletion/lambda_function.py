import json
import os

import boto3
from botocore.exceptions import ClientError

BUCKET = os.environ['BUCKET']
MAPPER_QUEUE = os.environ['MAPPER_QUEUE']
s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def _json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
    except ClientError as error:
        if error.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey'):
            return None
        raise


def _send(tasks):
    for start in range(0, len(tasks), 10):
        chunk = tasks[start:start + 10]
        response = sqs.send_message_batch(QueueUrl=MAPPER_QUEUE, Entries=[
            {'Id': str(start + index),
             'MessageBody': json.dumps(task, separators=(',', ':'))}
            for index, task in enumerate(chunk)
        ])
        if response.get('Failed') or len(response.get('Successful', [])) != len(chunk):
            raise RuntimeError('Failed to publish every Mapper task')


def _process(message):
    batch = _json(message['batchKey'])
    if not batch:
        raise ValueError('Extraction batch document is missing')
    if batch.get('schemaVersion') != 3:
        raise ValueError('Unsupported extraction batch schemaVersion')
    id_bits = int(batch['idBits'])
    if id_bits not in (32, 64):
        raise ValueError('Unsupported global ID width')
    record_bytes = 20 if id_bits == 64 else 16
    if int(batch['hydratedRecordBytes']) != record_bytes:
        raise ValueError('Batch hydrated-record width does not match ID width')

    dispatched_key = message['batchKey'].removesuffix('batch.json') + 'dispatched.json'
    if _json(dispatched_key) is not None:
        return True
    parts = []
    expected_parts = int(batch['expectedParts'])
    for part_id in range(expected_parts):
        marker = _json(f'{message["completionPrefix"]}/{part_id}/result.json')
        if marker is None:
            return False
        if (marker.get('schemaVersion') != 2
                or marker.get('batchId') != batch['batchId']
                or int(marker.get('partId', -1)) != part_id
                or int(marker.get('expectedParts', -1)) != expected_parts):
            raise ValueError('Extractor completion marker does not match its batch')
        parts.append(marker)

    for bucket in batch['missingBuckets']:
        fragments = []

        for part in parts:
            try:
                fragment = next(
                    item for item in part['buckets']
                    if int(item['sliceId']) == int(bucket['sliceId'])
                    and int(item['bucketId']) == int(bucket['bucketId'])
                )
            except StopIteration:
                raise ValueError(
                    'Extractor completion marker is missing a required bucket'
                )
            fragments.append(fragment)

        fragments.sort(key=lambda item: int(item['startId']))

        next_id = 0
        hydrated_records = 0

        for fragment in fragments:
            start_id = int(fragment['startId'])
            end_id = int(fragment['endId'])
            fragment_count = int(fragment['recordCount'])

            if start_id != next_id or end_id < start_id:
                raise ValueError(
                    'Extractor fragments do not form contiguous ID ranges'
                )

            if (int(fragment['idBits']) != id_bits
                    or int(fragment['recordBytes']) != record_bytes):
                raise ValueError(
                    'Extractor fragment format does not match its batch'
                )

            if fragment_count < 0:
                raise ValueError('Extractor fragment has a negative record count')

            if fragment_count and not fragment.get('key'):
                raise ValueError('Non-empty extractor fragment has no S3 key')

            next_id = end_id
            hydrated_records += fragment_count

        if next_id != int(batch['offtargetsCount']):
            raise ValueError(
                'Extractor fragments do not cover the off-target catalogue'
            )

        if hydrated_records != int(bucket['elementCount']):
            raise ValueError(
                'Hydrated record count does not match the compressed bucket count'
            )

        manifest = {
            'schemaVersion': 2,
            'sliceId': bucket['sliceId'],
            'bucketId': bucket['bucketId'],
            'idBits': id_bits,
            'recordFormat': {
                'endianness': 'little',
                'packing': 'packed',
                'recordBytes': record_bytes,
                'fields': [
                    'signature:uint64',
                    f'globalId:uint{id_bits}',
                    'occurrences:uint32',
                ],
            },
            'recordCount': hydrated_records,
            'parts': fragments,
        }

        s3.put_object(
            Bucket=BUCKET,
            Key=bucket['manifestKey'],
            Body=json.dumps(manifest, separators=(',', ':')).encode(),
            ContentType='application/json',
            Tagging='crackling-cache=extracted-candidates',
        )

    _send([
        {
            'schemaVersion': 1,
            'taskType': 'mapperReference',
            'bucket': BUCKET,
            'batchKey': message['batchKey'],
            'mapperTaskIndex': index,
        }
        for index in range(len(batch['mapperTasks']))
    ])

    s3.put_object(Bucket=BUCKET, Key=dispatched_key,
                    Body=json.dumps({'schemaVersion': 1,
                                    'batchId': batch['batchId']},
                                    separators=(',', ':')).encode(),
                    ContentType='application/json')
    return True


def lambda_handler(event, context):
    ready = 0
    messages = {}
    for record in event.get('Records', []):
        message = json.loads(record['body'])
        messages[message['batchKey']] = message
    for message in messages.values():
        ready += int(_process(message))
    return {'processed': len(event.get('Records', [])), 'ready': ready}
