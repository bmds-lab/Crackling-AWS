"""Persist schema-7 Mapper batch scores; scoring and deduplication stay in Mapper."""

import hashlib
import json
import math
import os
import random
import re
import time
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


TRANSACTION_MAX_ATTEMPTS = 16
TRANSACTION_BACKOFF_BASE_SECONDS = 0.1
TRANSACTION_BACKOFF_MAX_SECONDS = 2.0
MAX_ATOMIC_GUIDES = 98
TARGETS_TABLE = os.getenv('TARGETS_TABLE')
TASK_TRACKING_TABLE = os.getenv('TASK_TRACKING_TABLE')
BATCH_COMPLETIONS_TABLE = os.getenv('BATCH_COMPLETIONS_TABLE')
MAPPER_MARKER_PATTERN = re.compile(
    r'^(?P<genome>.+)/mapper/(?P<job_id>[^/]+)/batches/'
    r'(?P<batch_id>[^/]+)/mapper-result\.json$'
)

s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')


def _mapper_event(record):
    key = unquote_plus(record['s3']['object']['key'])
    match = MAPPER_MARKER_PATTERN.fullmatch(key)
    if not match:
        # Old per-guide/shard notifications must not enter batch finalization.
        return None
    values = match.groupdict()
    return {
        'bucket': record['s3']['bucket']['name'],
        'key': key,
        'genome': values['genome'],
        'jobId': values['job_id'],
        'batchId': values['batch_id'],
    }


def _s3_records(record):
    """Return S3 event records from either direct S3 or SQS delivery."""
    if 's3' in record:
        return [record]
    if record.get('eventSource') != 'aws:sqs':
        return []
    message = json.loads(record['body'])
    return message.get('Records', [])


def _get_json(bucket, key, missing_is_none=False):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        code = error.response.get('Error', {}).get('Code')
        if missing_is_none and code in ('404', 'NoSuchKey'):
            return None
        raise
    return json.loads(response['Body'].read())


def _finite_number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _validate_batch(trigger, marker):
    """Validate every row before any persistence, including disabled methods."""
    if not isinstance(marker, dict):
        raise ValueError('Mapper batch marker must be an object')
    expected = {
        'schemaVersion': 7,
        'status': 'complete',
        'jobId': trigger['jobId'],
        'batchId': trigger['batchId'],
        'genome': trigger['genome'],
    }
    for field, value in expected.items():
        if marker.get(field) != value:
            raise ValueError(f'Mapper batch marker has invalid {field}')
    if type(marker['schemaVersion']) is not int:
        raise ValueError('Mapper schemaVersion must be integer 7')

    scoring = marker.get('scoring')
    if not isinstance(scoring, dict):
        raise ValueError('Mapper batch marker requires scoring configuration')
    distance = scoring.get('maxDistance')
    threshold = scoring.get('scoreThreshold')
    method = scoring.get('scoreMethod')
    if type(distance) is not int or not 0 <= distance <= 4:
        raise ValueError('Mapper maxDistance must be an integer from 0 to 4')
    if not _finite_number(threshold) or not 0 <= threshold <= 100:
        raise ValueError('Mapper scoreThreshold must be finite from 0 to 100')
    if method not in ('mit', 'cfd', 'and', 'or', 'avg'):
        raise ValueError('Mapper scoreMethod is invalid')
    semantics = 'complete' if threshold == 0 else 'thresholded'
    if marker.get('scoreSemantics') != semantics:
        raise ValueError('Mapper scoreSemantics does not match its threshold')

    rows = marker.get('scores')
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_ATOMIC_GUIDES:
        raise ValueError('Mapper batch must contain between 1 and 98 guides')
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError('Mapper score row must be an object')
        target_id = row.get('targetId')
        if type(target_id) is not int or target_id < 0 or target_id in seen:
            raise ValueError('Mapper Target IDs must be unique nonnegative integers')
        seen.add(target_id)
        for name in ('mit', 'cfd'):
            score = row.get(name)
            disabled = (method == 'mit' and name == 'cfd') or (
                method == 'cfd' and name == 'mit'
            )
            if not _finite_number(score) or (
                score != -1 if disabled else not 0 <= score <= 100
            ):
                raise ValueError(f'Mapper target {target_id} has invalid {name} score')
    return marker


def _result_key(trigger):
    return (
        f'{trigger["genome"]}/reducer/{trigger["jobId"]}/batches/'
        f'{trigger["batchId"]}/result.json'
    )


def _write_json(bucket, key, document):
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(document, separators=(',', ':'), allow_nan=False).encode('utf-8'),
        ContentType='application/json',
    )


def _completion_id(trigger):
    return f'{trigger["jobId"]}:{trigger["targetId"]}'


def _batch_guard(trigger, source_digest, rows):
    return {
        'JobID': {'S': trigger['jobId']},
        'BatchID': {'S': trigger['batchId']},
        'SourceDigest': {'S': source_digest},
        'Genome': {'S': trigger['genome']},
        'GuideCount': {'N': str(len(rows))},
    }


def _guard_complete(guard):
    existing = dynamodb_client.get_item(
        TableName=BATCH_COMPLETIONS_TABLE,
        Key={name: guard[name] for name in ('JobID', 'BatchID')},
        ConsistentRead=True,
    ).get('Item', {})
    if not existing:
        return False
    if any(existing.get(name) != value for name, value in guard.items()):
        raise ValueError('Batch completion record conflicts with Mapper result')
    return True


def _completion_transaction(trigger, rows, guard):
    actions = []
    for row in rows:
        actions.append({'Update': {
            'TableName': TARGETS_TABLE,
            'Key': {'JobID': {'S': trigger['jobId']},
                    'TargetID': {'N': str(row['targetId'])}},
            'UpdateExpression': 'SET IsslScore = :mit, CfdScore = :cfd, ReducerTaskId = :task',
            'ConditionExpression': 'attribute_exists(JobID) AND attribute_not_exists(ReducerTaskId)',
            'ExpressionAttributeValues': {
                ':mit': {'S': json.dumps(row['mit'])},
                ':cfd': {'S': json.dumps(row['cfd'])},
                ':task': {'S': _completion_id({**trigger, 'targetId': row['targetId']})},
            },
        }})
    actions.append({'Update': {
        'TableName': TASK_TRACKING_TABLE,
        'Key': {'JobID': {'S': trigger['jobId']}},
        'UpdateExpression': 'ADD NumScoredOfftarget :count, #version :one',
        'ConditionExpression': 'attribute_exists(JobID)',
        'ExpressionAttributeNames': {'#version': 'Version'},
        'ExpressionAttributeValues': {':count': {'N': str(len(rows))}, ':one': {'N': '1'}},
    }})
    actions.append({'Put': {
        'TableName': BATCH_COMPLETIONS_TABLE,
        'Item': guard,
        'ConditionExpression': 'attribute_not_exists(JobID)',
    }})
    return actions


def _record_batch_completion(trigger, rows, source_digest, context=None):
    guard = _batch_guard(trigger, source_digest, rows)
    if _guard_complete(guard):
        return 0
    actions = _completion_transaction(trigger, rows, guard)
    for attempt in range(TRANSACTION_MAX_ATTEMPTS):
        try:
            dynamodb_client.transact_write_items(
                TransactItems=actions,
            )
            return len(rows)
        except ClientError as error:
            error_code = error.response.get('Error', {}).get('Code')
            reasons = error.response.get('CancellationReasons', [])
            codes = {reason.get('Code') for reason in reasons}
            if error_code == 'TransactionCanceledException':
                # A duplicate committed batch fails its guard condition.
                # Throttling/conflict cancellations need no extra guard read:
                # retrying the same conditional transaction remains safe.
                if 'ConditionalCheckFailed' in codes:
                    if _guard_complete(guard):
                        return 0
                    raise
                if not codes or codes - {
                    'None', 'TransactionConflict', 'ThrottlingError',
                    'ProvisionedThroughputExceeded',
                }:
                    raise
                throttled = bool(codes & {'ThrottlingError', 'ProvisionedThroughputExceeded'})
            elif error_code in {
                'ThrottlingException', 'ProvisionedThroughputExceededException',
                'RequestLimitExceeded',
            }:
                throttled = True
            else:
                raise
            if attempt + 1 == TRANSACTION_MAX_ATTEMPTS:
                raise
            delay = random.uniform(0, min(
                TRANSACTION_BACKOFF_MAX_SECONDS,
                TRANSACTION_BACKOFF_BASE_SECONDS * (2 ** attempt),
            ))
            if (context is not None and hasattr(context, 'get_remaining_time_in_millis')
                    and context.get_remaining_time_in_millis() < (delay + 2) * 1000):
                raise TimeoutError('Insufficient time remaining to retry batch completion')
            print(json.dumps({
                'event': 'reducer_transaction_throttled' if throttled else 'reducer_transaction_conflict',
                'jobId': trigger['jobId'],
                'batchId': trigger['batchId'],
                'attempt': attempt + 1, 'retryDelaySeconds': delay,
                'errorCode': error_code,
                'throttlingReasons': error.response.get('ThrottlingReasons', []),
                'cancellationReasons': reasons,
            }))
            time.sleep(delay)



def _process_s3_record(record, context=None):
    trigger = _mapper_event(record)
    if trigger is None:
        return 'ignored'
    marker = _validate_batch(trigger, _get_json(trigger['bucket'], trigger['key']))
    # Detect replacement/corruption of a deterministic Mapper result key.
    source_digest = hashlib.sha256(json.dumps(
        marker, sort_keys=True, separators=(',', ':'), allow_nan=False,
    ).encode('utf-8')).hexdigest()
    result = {
        'schemaVersion': 7,
        'status': 'complete',
        'jobId': trigger['jobId'],
        'batchId': trigger['batchId'],
        'genome': trigger['genome'],
        'sourceMapperKey': trigger['key'],
        'sourceDigest': source_digest,
        'guideCount': len(marker['scores']),
        'scoring': marker['scoring'],
        'scoreSemantics': marker['scoreSemantics'],
    }
    key = _result_key(trigger)
    existing = _get_json(trigger['bucket'], key, missing_is_none=True)
    if existing is not None:
        if existing != result:
            raise ValueError('Batch finalization marker conflicts with Mapper result')
        return 'processed'

    if not TARGETS_TABLE or not TASK_TRACKING_TABLE or not BATCH_COMPLETIONS_TABLE:
        raise ValueError('Reducer DynamoDB table environment is not configured')
    updated = _record_batch_completion(trigger, marker['scores'], source_digest, context)
    # Publish after the atomic commit; the durable guard protects S3 retries.
    _write_json(trigger['bucket'], key, result)
    print(json.dumps({
        'event': 'reducer_batch_complete',
        'jobId': trigger['jobId'],
        'batchId': trigger['batchId'],
        'guideCount': len(marker['scores']),
        'targetsUpdated': updated,
    }))
    return 'processed'


def lambda_handler(event, context):
    counts = {'processed': 0, 'ignored': 0}
    batch_item_failures = []
    for envelope in event.get('Records', []):
        try:
            for record in _s3_records(envelope):
                counts[_process_s3_record(record, context)] += 1
        except Exception as error:
            message_id = envelope.get('messageId')
            if message_id is None:
                raise
            print(json.dumps({
                'event': 'reducer_message_failed',
                'messageId': message_id,
                'errorType': type(error).__name__,
                'error': str(error),
            }))
            batch_item_failures.append({'itemIdentifier': message_id})
    counts['batchItemFailures'] = batch_item_failures
    return counts
