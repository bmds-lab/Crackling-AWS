import json
import os
import shutil
import subprocess
import tempfile
import struct
import boto3

BUCKET = os.environ['BUCKET']
COMPLETION_QUEUE = os.environ['COMPLETION_QUEUE']
BUCKET_MERGE_GAP_BYTES = int(os.getenv('BUCKET_MERGE_GAP_BYTES', str(256 * 1024)))
MAX_MERGE_RANGE_BYTES = int(os.getenv('MAX_MERGE_RANGE_BYTES', str(16 * 1024 * 1024)))
BINARY_SOURCE = '/opt/extractor'
BINARY = '/tmp/extractor'
CHUNK = 8 * 1024 * 1024
s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def _range(source, path):
    start, end = int(source['startByte']), int(source['endByte'])
    if end <= start:
        open(path, 'wb').close()
        return
    response = s3.get_object(Bucket=source.get('bucket', BUCKET), Key=source['key'],
                             Range=f'bytes={start}-{end - 1}')
    with open(path, 'wb') as output:
        while True:
            data = response['Body'].read(CHUNK)
            if not data:
                break
            output.write(data)
    if os.path.getsize(path) != end - start:
        raise ValueError(f'Truncated S3 range for {source["key"]}')


def _download_overflow(catalogue, start_id, end_id, path):
    table = catalogue['overflow']
    base = int(table['startByte'])
    count = int(table['recordCount'])
    if (base < 0 or count < 0 or int(table['recordBytes']) != 16
            or int(table['endByte']) != base + 16 * count):
        raise ValueError('Invalid overflow table contract')
    def lower_bound(target):
        lo, hi = 0, count
        while lo < hi:
            mid = (lo + hi) // 2
            offset = base + 16 * mid
            response = s3.get_object(
                Bucket=catalogue['bucket'], Key=catalogue['key'],
                Range=f'bytes={offset}-{offset + 7}')
            body = response['Body']
            try:
                data = body.read()
            finally:
                body.close()
            if len(data) != 8:
                raise ValueError('Truncated overflow ID')
            global_id, = struct.unpack('<Q', data)
            if global_id < target:
                lo = mid + 1
            else:
                hi = mid
        return lo

    first = lower_bound(start_id)
    last = lower_bound(end_id)
    if last < first or last - first > end_id - start_id:
        raise ValueError('Invalid overflow partition size')
    _range({
        'bucket': catalogue['bucket'], 'key': catalogue['key'],
        'startByte': base + 16 * first,
        'endByte': base + 16 * last,
    }, path)

def _merged_bucket_groups(buckets):
    if BUCKET_MERGE_GAP_BYTES < 0 or MAX_MERGE_RANGE_BYTES < 1:
        raise ValueError('Invalid bucket merge settings')

    ordered = sorted(buckets, key=lambda item: int(item['startByte']))
    group = []
    group_start = group_end = None

    for bucket in ordered:
        start = int(bucket['startByte'])
        end = int(bucket['endByte'])
        if start < 0 or end < start:
            raise ValueError('Invalid bucket byte range')

        if not group:
            group, group_start, group_end = [bucket], start, end
            continue

        if start < group_end:
            raise ValueError('Overlapping bucket byte ranges')

        merged_end = end
        if (start - group_end <= BUCKET_MERGE_GAP_BYTES
                and merged_end - group_start <= MAX_MERGE_RANGE_BYTES):
            group.append(bucket)
            group_end = merged_end
        else:
            yield group_start, group_end, group
            group, group_start, group_end = [bucket], start, end

    if group:
        yield group_start, group_end, group


def _read_merged_range(source, start, end):
    if end == start:
        return b''

    response = s3.get_object(
        Bucket=source['bucket'],
        Key=source['key'],
        Range=f'bytes={start}-{end - 1}',
    )
    body = response['Body']
    try:
        data = body.read()
    finally:
        body.close()

    if len(data) != end - start:
        raise ValueError(f'Truncated S3 range for {source["key"]}')
    return data

def _process(task):
    # Older queued tasks also identify the job in their extraction object key.
    job_id = task.get('jobId') or (
        task.get('batchKey', '').partition('/issl/extractions/')[2].split('/', 1)[0]
    ) or None
    print(json.dumps({
        'event': 'extractor_start',
        'jobId': job_id,
        'batchId': task.get('batchId'),
        'partId': task.get('partId'),
    }))
    if task.get('schemaVersion') != 3:
        raise ValueError('Unsupported Extractor task schemaVersion')
    id_bits = int(task['idBits'])
    record_bytes = int(task['hydratedRecordBytes'])
    if id_bits not in (32, 64) or record_bytes != 12 + id_bits // 8:
        raise ValueError('Extractor hydrated-record width does not match ID width')
    if not os.path.exists(BINARY):
        shutil.copyfile(BINARY_SOURCE, BINARY)
        os.chmod(BINARY, 0o755)
    records = []
    with tempfile.TemporaryDirectory(dir='/tmp') as directory:
        signatures = os.path.join(directory, 'signatures.bin')
        occurrences = os.path.join(directory, 'occurrences.bin')
        overflow = os.path.join(directory, 'overflow.bin')
        bucket_file = os.path.join(directory, 'bucket.bin')
        output_file = os.path.join(directory, 'candidates.bin')
        common = {'bucket': task['catalogue']['bucket'],
                  'key': task['catalogue']['key']}
        _range({**common, **task['catalogue']['signatures']}, signatures)
        _range({**common, **task['catalogue']['occurrences']}, occurrences)
        _download_overflow(task['catalogue'], int(task['idRange']['start']),
                           int(task['idRange']['end']), overflow)
        catalogue_bytes = sum(os.path.getsize(path)
                              for path in (signatures, occurrences, overflow))
        total_bucket_bytes = 0
        total_output_bytes = 0
        peak_local_bytes = catalogue_bytes
        previous_output_bytes = 0
        staged = []
        plan_file = os.path.join(directory, 'buckets.tsv')

        for start, end, group in _merged_bucket_groups(task['buckets']):
            oversized = end - start > MAX_MERGE_RANGE_BYTES
            data = None if oversized else _read_merged_range(common, start, end)

            for bucket in group:
                index = len(staged)
                bucket_file = os.path.join(directory, f'bucket-{index}.bin')
                output_file = os.path.join(directory, f'candidates-{index}.bin')

                if oversized:
                    _range({
                        **common, 'startByte': bucket['startByte'],
                        'endByte': bucket['endByte'],
                    }, bucket_file)
                else:
                    local_start = int(bucket['startByte']) - start
                    local_end = int(bucket['endByte']) - start
                    with open(bucket_file, 'wb') as output:
                        output.write(data[local_start:local_end])

                staged.append((bucket, bucket_file, output_file))

        with open(plan_file, 'w', encoding='utf-8') as plan:
            for bucket, bucket_file, output_file in staged:
                plan.write(
                    f"{bucket_file}\t{bucket['elementCount']}"
                    f'\t{output_file}\n')

        completed = subprocess.run([
            BINARY, signatures, occurrences, overflow, plan_file,
            str(task['idRange']['start']),
            str(task['idRange']['end']), 
            str(task['idBits']),
            str(task['offtargetsCount']),
            ], capture_output=True, text=True, check=False)

        if completed.returncode:
            raise RuntimeError(f'Extractor failed: {completed.stderr}')

        for bucket, bucket_file, output_file in staged:
            bucket_bytes = os.path.getsize(bucket_file)
            size = os.path.getsize(output_file)
            if size % record_bytes:
                raise ValueError('Extractor output is not aligned to record format')

            key = (f'{bucket["cachePrefix"]}/parts/'
                   f'{task["idRange"]["start"]}-{task["idRange"]["end"]}.bin')
            count = size // record_bytes
            total_bucket_bytes += bucket_bytes
            total_output_bytes += size
            peak_local_bytes = catalogue_bytes + total_output_bytes + total_bucket_bytes

            print(json.dumps({
                'event': 'extractor_bucket_sizes',
                'jobId': job_id,
                'batchId': task['batchId'],
                'partId': task['partId'],
                'sliceId': bucket['sliceId'],
                'bucketId': bucket['bucketId'],
                'catalogueBytes': catalogue_bytes,
                'bucketBytes': bucket_bytes,
                'hydratedOutputBytes': size,
                'recordBytes': record_bytes,
                'idBits': id_bits,
            }))
            if count:
                s3.upload_file(output_file, BUCKET, key,
                               ExtraArgs={
                                   'ContentType': 'application/octet-stream',
                                   'Tagging': 'crackling-cache=extracted-candidates',
                               })

            records.append({'sliceId': bucket['sliceId'], 'bucketId': bucket['bucketId'],
                            'manifestKey': bucket['manifestKey'], 'key': key if count else None,
                            'startId': task['idRange']['start'], 'endId': task['idRange']['end'],
                            'recordCount': count, 'recordBytes': record_bytes, 'idBits': id_bits})
        print(json.dumps({
            'event': 'extractor_part_sizes',
            'jobId': job_id,
            'batchId': task['batchId'],
            'partId': task['partId'],
            'expectedParts': task['expectedParts'],
            'catalogueBytes': catalogue_bytes,
            'bucketBytesProcessed': total_bucket_bytes,
            'hydratedOutputBytes': total_output_bytes,
            'peakLocalBytes': peak_local_bytes,
        }))
    marker_key = f'{task["completionPrefix"]}/{task["partId"]}/result.json'
    marker = {'schemaVersion': 2, 'batchId': task['batchId'], 'partId': task['partId'],
              'expectedParts': task['expectedParts'], 'buckets': records}
    s3.put_object(Bucket=BUCKET, Key=marker_key,
                  Body=json.dumps(marker, separators=(',', ':')).encode(),
                  ContentType='application/json')
    sqs.send_message(QueueUrl=COMPLETION_QUEUE,
                     MessageBody=json.dumps({'batchKey': task['batchKey'],
                                             'completionPrefix': task['completionPrefix']}))
    print(json.dumps({
        'event': 'extractor_complete',
        'jobId': job_id,
        'batchId': task['batchId'],
        'partId': task['partId'],
    }))


def lambda_handler(event, context):
    for record in event.get('Records', []):
        message = json.loads(record['body'])

        if message.get('taskType') == 'extractorReference':
            if message.get('schemaVersion') != 1:
                raise ValueError('Unsupported extractor reference version')

            batch = json.loads(s3.get_object(
                Bucket=message['bucket'],
                Key=message['batchKey'],
            )['Body'].read())

            index = message['extractorTaskIndex']
            tasks = batch['extractorTasks']
            if type(index) is not int or not 0 <= index < len(tasks):
                raise ValueError('Invalid extractor task index')

            task = tasks[index]
        else:
            # Cached batch.json file
            task = message

        _process(task)

    return {'processed': len(event.get('Records', []))}
