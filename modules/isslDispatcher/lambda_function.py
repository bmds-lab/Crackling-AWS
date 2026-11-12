import hashlib
import json
import os

import boto3
from botocore.exceptions import ClientError

BUCKET = os.environ['BUCKET']
MAPPER_QUEUE = os.environ['MAPPER_QUEUE']
EXTRACTOR_QUEUE = os.environ['EXTRACTOR_QUEUE']
MAX_EXTRACTORS = int(os.getenv('MAX_EXTRACTORS', '50'))
EXTRACTOR_SAFE_BYTES = int(os.getenv('EXTRACTOR_SAFE_BYTES', str(8 * 1024 ** 3)))
MAPPER_SAFE_BYTES = int(os.getenv('MAPPER_SAFE_BYTES', str(8 * 1024 ** 3)))
# 95 target updates + progress update + completion guard use 97 of 100 actions.
MAX_MAPPER_GUIDES = int(os.getenv('MAX_GUIDES_PER_MAPPER', '95'))
MAX_EXTRACTION_GUIDES = int(os.getenv('MAX_GUIDES_PER_EXTRACTION_GROUP', '100'))
MAX_DISTANCE = int(os.getenv('MAX_DISTANCE', '4'))
SCORE_THRESHOLD = float(os.getenv('SCORE_THRESHOLD', '75'))
SCORE_METHOD = os.getenv('SCORE_METHOD', 'and')
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

if min(MAX_EXTRACTORS, EXTRACTOR_SAFE_BYTES, MAPPER_SAFE_BYTES,
       MAX_EXTRACTION_GUIDES, MAX_MAPPER_GUIDES) < 1:
    raise ValueError('Extractor limits and maximum guide batch size must be positive')

if MAX_MAPPER_GUIDES > 98:
    raise ValueError('MAX_GUIDES_PER_MAPPER must not exceed 98 for atomic finalization')


def _json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
    except ClientError as error:
        if error.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey'):
            return None
        raise


def _hash(*parts):
    return hashlib.sha256(':'.join(map(str, parts)).encode()).hexdigest()


def _ceil_div(numerator, denominator):
    return (numerator + denominator - 1) // denominator


def _largest_manifest_bucket(manifest):
    largest = 0
    for shard in manifest['shards']:
        offsets = [int(value) for value in shard['bucketOffsets']]
        if any(end < start for start, end in zip(offsets, offsets[1:])):
            raise ValueError('ISSL bucket offsets must be monotonic')
        for start, end in zip(offsets, offsets[1:]):
            largest = max(largest, end - start)
    return largest

def _catalogue_partition_bytes(partition_records, overflow_count):
    return 6 * partition_records + 16 * min(overflow_count, partition_records)


def _extractor_allocation(manifest):
    """Choose stable ID partitions using immutable, whole-ISSL metadata."""
    layout = manifest['layout']
    offtarget_count = int(layout['offtargetsCount'])
    overflow_count = int(layout['overflowCount'])
    catalogue_bytes = 6 * offtarget_count + 16 * overflow_count
    hydrated_record_bytes = int(layout['hydratedRecordBytes'])
    largest_bucket_bytes = _largest_manifest_bucket(manifest)

    max_extractor_count = max(
        1,
        min(MAX_EXTRACTORS, offtarget_count) if offtarget_count else 1,
    )

    required = max_extractor_count + 1

    for extractor_count in range(1, max_extractor_count + 1):
        partition_records = _ceil_div(offtarget_count, extractor_count)
        catalogue_part_bytes = _catalogue_partition_bytes(partition_records,overflow_count)

        bucket_working_bytes = max((
            int(bucket_end) - int(bucket_start)
            + min(int(entry_count), partition_records) * hydrated_record_bytes
            for shard in manifest['shards']
            for bucket_start, bucket_end, entry_count in zip(
                shard['bucketOffsets'],
                shard['bucketOffsets'][1:],
                shard['bucketEntryCounts'],
            )
        ), default=0)

        estimated_peak_bytes = catalogue_part_bytes + bucket_working_bytes

        if estimated_peak_bytes < EXTRACTOR_SAFE_BYTES:
            required = extractor_count
            break

    extractor_count = min(required, max_extractor_count)

    return {
        'extractorCount': extractor_count,
        'requiredExtractors': required,
        'offtargetsCount': offtarget_count,
        'overflowCount': overflow_count,
        'catalogueBytes': catalogue_bytes,
        'largestIsslBucketBytes': largest_bucket_bytes,
        'hydratedRecordBytes': hydrated_record_bytes,
    }

def _check_selected_bucket_feasibility(missing, allocation):
    largest = max(
        (int(bucket['endByte']) - int(bucket['startByte']) for bucket in missing),
        default=0,
    )
    extractor_count = allocation['extractorCount']
    partition_records = _ceil_div(
        allocation['offtargetsCount'],
        allocation['extractorCount'],
    )
    catalogue_part_bytes = _catalogue_partition_bytes(partition_records, int(allocation['overflowCount']))

    bucket_working_bytes = sum(
    int(bucket['endByte']) - int(bucket['startByte']) 
    + min(int(bucket['elementCount']), partition_records) 
    * allocation['hydratedRecordBytes']
    for bucket in missing
    )

    estimated_peak_bytes = catalogue_part_bytes  + bucket_working_bytes
    details = {
        'extractorCount': extractor_count,
        'requiredExtractors': allocation['requiredExtractors'],
        'offtargetsCount': allocation['offtargetsCount'],
        'catalogueBytes': allocation['catalogueBytes'],
        'catalogueBytesPerExtractor': catalogue_part_bytes,
        'largestIsslBucketBytes': allocation['largestIsslBucketBytes'],
        'largestSelectedBucketBytes': largest,
        'stagedBucketFootprintBytes': bucket_working_bytes,
        'estimatedPeakBytesPerExtractor': estimated_peak_bytes,
        'safeLimitBytes': EXTRACTOR_SAFE_BYTES,
        'maxExtractors': MAX_EXTRACTORS,
    }
    if estimated_peak_bytes < EXTRACTOR_SAFE_BYTES:
        print(json.dumps({'event': 'extractor_allocation', **details}))
        return

    reason = (
        'TOTAL_BUCKET_EXCEEDS_LAMBDA_LIMIT'
        if bucket_working_bytes >= EXTRACTOR_SAFE_BYTES
        else 'RETRY_WITH_MORE_EXTRACTORS'
    )
    print(json.dumps({
        'event': 'extractor_feasibility_failure',
        'reason': reason,
        **details,
    }))
    raise ValueError(
        f'{reason}: estimated extractor peak {estimated_peak_bytes} bytes '
        f'is not below safe limit {EXTRACTOR_SAFE_BYTES} bytes'
    )


def _signature(sequence):
    value = 0
    sequence = sequence[:20].upper()
    if len(sequence) != 20 or any(base not in 'ACGT' for base in sequence):
        raise ValueError('Guide must begin with exactly 20 A/C/G/T bases')
    for index, base in enumerate(sequence):
        value |= 'ACGT'.index(base) << (index * 2)
    return value

def _masked_bucket(sequence, mask):
    signature = _signature(sequence)
    value = 0
    selected = 0
    for position in range(20):
        if int(mask) & (1 << position):
            value |= ((signature >> (2 * position)) & 3) << (2 * selected)
            selected += 1
    return value

def _send(queue, tasks):
    for batch_start in range(0, len(tasks), 10):
        batch = tasks[batch_start:batch_start + 10]
        response = sqs.send_message_batch(QueueUrl=queue, Entries=[
            {'Id': str(batch_start + index),
             'MessageBody': json.dumps(task, separators=(',', ':'))}
            for index, task in enumerate(batch)
        ])
        if response.get('Failed') or len(response.get('Successful', [])) != len(batch):
            raise RuntimeError('SQS did not accept every task')


def _mapper_tasks(guides, genome, manifest, selected):
    guides = sorted(guides, key=lambda item: int(item['TargetID']))
    job_id = str(guides[0]['JobID'])
    shards = manifest['shards']
    selected_by_key = {
        (int(item['sliceId']), int(item['bucketId'])): item
        for item in selected
    }
    buckets = []
    bucket_indexes = {}
    # Slice order controls scoring traversal, even for nonconsecutive slice IDs.
    for shard in shards:
        slice_id = int(shard['sliceId'])
        required_buckets = sorted({
            _masked_bucket(guide['Sequence'], shard['mask'])
            for guide in guides
        })
        for bucket_id in required_buckets:
            key = (slice_id, bucket_id)
            bucket_indexes[key] = len(buckets)
            buckets.append({
                'sliceId': slice_id, 'bucketId': bucket_id,
                'manifestKey': selected_by_key[key]['manifestKey'],
            })
    contracts = []
    for guide in guides:
        contracts.append({
            'targetId': int(guide['TargetID']),
            'guideSequence': guide['Sequence'][:20].upper(),
            'bucketIndexes': [
                bucket_indexes[(int(shard['sliceId']),
                                _masked_bucket(guide['Sequence'], shard['mask']))]
                for shard in shards
            ],
        })
    task = {
        'schemaVersion': 7, 'taskType': 'mapperBatch',
        'jobId': job_id, 'genome': genome,
        'guides': contracts, 'buckets': buckets,
        'idBits': int(manifest['layout']['idBits']),
        'hydratedRecordBytes': int(manifest['layout']['hydratedRecordBytes']),
        'plannedHydratedBytes': sum(
            int(shard['bucketEntryCounts'][bucket['bucketId']])
            * int(manifest['layout']['hydratedRecordBytes'])
            for shard in shards
            for bucket in buckets
            if int(shard['sliceId']) == bucket['sliceId']
        ),
        'scoring': {
            'maxDistance': MAX_DISTANCE,
            'scoreThreshold': SCORE_THRESHOLD,
            'scoreMethod': SCORE_METHOD,
        },
        'output': {'bucket': BUCKET},
    }
    # Hash the complete immutable scoring input, not only guide IDs. Changing
    # sequence, selection, width or scoring configuration must not reuse output.
    batch_id = hashlib.sha256(json.dumps(
        task, sort_keys=True, separators=(',', ':'), allow_nan=False,
    ).encode()).hexdigest()
    task['batchId'] = batch_id
    task['output']['metadataKey'] = (
        f'{genome}/mapper/{job_id}/batches/{batch_id}/mapper-result.json'
    )
    return [task]

def _partition_guides(guides, manifest):
    """Bound guide count and distinct hydrated bytes across all selected slices."""
    guides = sorted(guides, key=lambda item: int(item['TargetID']))
    record_bytes = int(manifest['layout']['hydratedRecordBytes'])
    shards = manifest['shards']
    groups, group = [], []
    bucket_ids = set()
    size = 0
    for guide in guides:
        selected = [
            _masked_bucket(guide['Sequence'], shard['mask'])
            for shard in shards
        ]
        hydrated_bytes = [
            int(shard['bucketEntryCounts'][bucket_id]) * record_bytes
            for shard, bucket_id in zip(shards, selected)
        ]
        if sum(hydrated_bytes) > MAPPER_SAFE_BYTES:
            raise ValueError(
                f'Guide {guide["TargetID"]} exceeds Mapper safe input limit '
                f'{MAPPER_SAFE_BYTES} bytes'
            )
        keys = [
            (int(shard['sliceId']), bucket_id)
            for shard, bucket_id in zip(shards, selected)
        ]
        added_bytes = sum(
            count for key, count in zip(keys, hydrated_bytes)
            if key not in bucket_ids
        )
        if group and (len(group) >= MAX_MAPPER_GUIDES
                      or size + added_bytes > MAPPER_SAFE_BYTES):
            groups.append(group)
            group = []
            bucket_ids = set()
            size = 0

        group.append(guide)
        for key, count in zip(keys, hydrated_bytes):
            if key not in bucket_ids:
                size += count
                bucket_ids.add(key)
    if group:
        groups.append(group)
    return groups

def _valid_cached_bucket(manifest_key, slice_id, bucket_id, layout):
    cached = _json(manifest_key)
    if not isinstance(cached, dict):
        return False

    record_format = cached.get('recordFormat')
    if not isinstance(record_format, dict):
        return False

    try:
        return (
            cached.get('schemaVersion') == 2
            and int(cached.get('sliceId')) == slice_id
            and int(cached.get('bucketId')) == bucket_id
            and int(cached.get('idBits')) == int(layout['idBits'])
            and int(record_format.get('recordBytes'))
                == int(layout['hydratedRecordBytes'])
        )
    except (TypeError, ValueError):
        return False

def _selected_buckets(guides, genome, manifest):
    selected = []
    for shard in manifest['shards']:
        slice_id = int(shard['sliceId'])
        mask = int(shard['mask'])
        offsets = shard['bucketOffsets']
        entry_counts = shard['bucketEntryCounts']

        bucket_ids = sorted({
            _masked_bucket(guide['Sequence'], mask)
            for guide in guides
        })

        for bucket_id in bucket_ids:
            prefix = f'{genome}/issl/cache/slices/{slice_id}/buckets/{bucket_id}'
            manifest_key = f'{prefix}/manifest.json'

            selected.append({
                'sliceId': slice_id,
                'bucketId': bucket_id,
                'startByte': int(offsets[bucket_id]),
                'endByte': int(offsets[bucket_id + 1]),
                'elementCount': int(entry_counts[bucket_id]),
                'cachePrefix': prefix,
                'manifestKey': manifest_key,
                'cached': _valid_cached_bucket(
                manifest_key,
                slice_id,
                bucket_id,
                manifest['layout'],
            ),
            })

    return selected


def _extraction_bucket_budget(allocation):
    partition_records = _ceil_div(
        allocation['offtargetsCount'], allocation['extractorCount']
    )
    catalogue_part_bytes = _catalogue_partition_bytes(
        partition_records, int(allocation['overflowCount'])
    )
    return max(0, EXTRACTOR_SAFE_BYTES - catalogue_part_bytes)


def _partition_extraction_guides(guides, genome, manifest):
    """Bound cumulative raw bucket work, reusing cache checks within this event."""
    guides = sorted(guides, key=lambda item: int(item['TargetID']))
    selected = _selected_buckets(guides, genome, manifest)
    by_bucket = {(item['sliceId'], item['bucketId']): item for item in selected}
    record_bytes = int(manifest['layout']['hydratedRecordBytes'])

    bucket_work_bytes = {
        key: (
            int(bucket['endByte']) - int(bucket['startByte'])
            + int(bucket['elementCount']) * record_bytes
        )
        for key, bucket in by_bucket.items()
    }
    allocation = None
    budget = 0
    if any(not item['cached'] for item in selected):
        allocation = _extractor_allocation(manifest)
        budget = _extraction_bucket_budget(allocation)

    group, keys, missing_bytes = [], set(), 0
    for guide in guides:
        guide_keys = {
            (int(shard['sliceId']),
             _masked_bucket(guide['Sequence'], shard['mask']))
            for shard in manifest['shards']
        }
        added_bytes = sum(
            bucket_work_bytes[key]
            for key in guide_keys - keys
            if not by_bucket[key]['cached']
        )

        if group and (missing_bytes + added_bytes > budget
                      or len(group) >= MAX_EXTRACTION_GUIDES):
            yield group, [by_bucket[key] for key in sorted(keys)], allocation
            group, keys, missing_bytes = [], set(), 0
            added_bytes = sum(
                bucket_work_bytes[key]
                for key in guide_keys - keys
                if not by_bucket[key]['cached']
            )
        # A single guide may exceed the work budget: keep it whole and let the
        # existing per-extractor storage feasibility check decide if it can run.
        group.append(guide)
        keys.update(guide_keys)
        missing_bytes += added_bytes
    if group:
        yield group, [by_bucket[key] for key in sorted(keys)], allocation


def _dispatch_group(guides, genome, manifest, selected=None, allocation=None):
    if selected is None:
        selected = _selected_buckets(guides, genome, manifest)
    # Share extraction, then size mapper groups using their required buckets.
    mapper_tasks = [
        task
        for group in _partition_guides(guides, manifest)
        for task in _mapper_tasks(group, genome, manifest, selected)
    ]
    missing = [{key: value for key, value in item.items() if key != 'cached'}
               for item in selected if not item['cached']]

    if allocation is None:
        allocation = _extractor_allocation(manifest)
    print(json.dumps({
        'event': 'extraction_guide_batch',
        'jobId': str(guides[0]['JobID']), 'genome': genome,
        'guideCount': len(guides), 'missingBucketCount': len(missing),
        'missingBucketBytes': sum(int(b['endByte']) - int(b['startByte']) for b in missing),
        'bucketBudgetBytes': _extraction_bucket_budget(allocation),
    }))
    _check_selected_bucket_feasibility(missing, allocation)
    extractor_count = allocation['extractorCount']

    job_id = str(guides[0]['JobID'])
    batch_id = _hash(
        job_id,
        *(int(g['TargetID']) for g in guides),
        f'extractors={extractor_count}',
        *(task['batchId'] for task in mapper_tasks),
    )
    prefix = f'{genome}/issl/extractions/{job_id}/{batch_id}'
    batch_key = f'{prefix}/batch.json'

    # All buckets are already cached/hydrated
    if not missing:
        mapper_key = f'{prefix}/mapper-plan.json'
        s3.put_object(
            Bucket=BUCKET,
            Key=mapper_key,
            Body=json.dumps({
                'schemaVersion': 3,
                'mapperTasks': mapper_tasks,
            }, separators=(',',':')).encode(),
            ContentType='application/json',
        )

        _send(MAPPER_QUEUE, [
            {
                'schemaVersion': 1,
                'taskType': 'mapperReference',
                'bucket': BUCKET,
                'batchKey': mapper_key,
                'mapperTaskIndex': index,
            }
            for index in range(len(mapper_tasks))
        ])
        return

    count = int(manifest['layout']['offtargetsCount'])
    batch = {
        'batchId': batch_id,
        'expectedParts': extractor_count,
        'offtargetsCount': count,
        'missingBuckets': missing,
        'mapperTasks': mapper_tasks,
    }
    tasks = []

    layout = manifest['layout']
    record_bytes = int(layout['hydratedRecordBytes'])
    batch.update({
        'schemaVersion': 3,
        'idBits': int(layout['idBits']),
        'hydratedRecordBytes': record_bytes,
    })

    for part_id in range(extractor_count):
        start_id = part_id * count // extractor_count
        end_id = (part_id + 1) * count // extractor_count
        tasks.append({
            'schemaVersion': 3,
            'jobId': job_id,
            'batchId': batch_id,
            'batchKey': batch_key,
            'partId': part_id,
            'expectedParts': extractor_count,
            'offtargetsCount': count,
            'idRange': {'start': start_id, 'end': end_id},
            'idBits': int(layout['idBits']),
            'hydratedRecordBytes': record_bytes,
            'catalogue': {
                'bucket': manifest['issl']['bucket'],
                'key': manifest['issl']['key'],
                'signatures': {
                    'startByte': int(layout['signatureOffsetBytes']) + 5 * start_id,
                    'endByte': int(layout['signatureOffsetBytes']) + 5 * end_id,
                },
                'occurrences': {
                    'startByte': int(layout['occurrenceOffsetBytes']) + start_id,
                    'endByte': int(layout['occurrenceOffsetBytes']) + end_id,
                },
                'overflow': {
                    'startByte': int(layout['overflowOffsetBytes']),
                    'endByte': int(layout['overflowOffsetBytes'])
                            + 16 * int(layout['overflowCount']),
                    'recordCount': int(layout['overflowCount']),
                    'recordBytes': 16,
                },
            },
            'buckets': missing,
            'completionPrefix': f'{prefix}/parts',
        })

    batch['extractorTasks'] = tasks

    s3.put_object(Bucket=BUCKET, Key=batch_key,
                Body=json.dumps(batch, separators=(',', ':')).encode(),
                ContentType='application/json')

    _send(EXTRACTOR_QUEUE, [
        {
            'schemaVersion': 1,
            'taskType': 'extractorReference',
            'bucket': BUCKET,
            'batchKey': batch_key,
            'extractorTaskIndex': index,
        }
        for index in range(len(tasks))
    ])


def lambda_handler(event, context):
    grouped = {}
    for record in event.get('Records', []):
        body = json.loads(record['body'])
        guide = json.loads(body['default'])
        genome = str(json.loads(body['genome']))
        target_id = int(guide['TargetID'])
        unique = grouped.setdefault((str(guide['JobID']), genome), {})
        if target_id in unique and unique[target_id] != guide:
            raise ValueError(f'Conflicting duplicate target ID: {target_id}')
        unique[target_id] = guide
    batches = 0
    for (_, genome), guides_by_id in grouped.items():
        manifest = _json(f'{genome}/issl/shards.json')
        if not manifest or manifest.get('schemaVersion') != 4:
            raise ValueError('Missing or unsupported ISSL shard manifest')
        guides = sorted(guides_by_id.values(), key=lambda item: int(item['TargetID']))
        for group, selected, allocation in _partition_extraction_guides(guides, genome, manifest):
            _dispatch_group(group, genome, manifest, selected, allocation)
            batches += 1
    return {'processedGuides': len(event.get('Records', [])), 'dispatchedBatches': batches}
