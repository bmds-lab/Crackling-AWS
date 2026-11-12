"""Score a guide batch against its selected hydrated buckets across all slices."""
import hashlib
import json
import math
import os
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.exceptions import ClientError


MAPPER_BINARY_SOURCE = '/opt/mapper'
MAPPER_BINARY = '/tmp/mapper'
COPY_CHUNK_BYTES = 8 * 1024 * 1024
DOWNLOAD_WORKERS = int(os.getenv('MAPPER_DOWNLOAD_WORKERS', '4'))
if not 1 <= DOWNLOAD_WORKERS <= 16:
    raise ValueError('MAPPER_DOWNLOAD_WORKERS must be an integer from 1 to 16')
MAPPER_SAFE_BYTES = int(os.getenv('MAPPER_SAFE_BYTES', str(8 * 1024 ** 3)))
s3_client = boto3.client('s3')


def _integer(value, name, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError(f'{name} must be an integer >= {minimum}')
    return value


def _string(value, name):
    if not isinstance(value, str) or not value:
        raise ValueError(f'{name} must be a nonempty string')
    return value


def _read_json(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response['Body']
    try:
        return json.loads(body.read())
    finally:
        body.close()


def _validate_scoring(scoring):
    if not isinstance(scoring, dict):
        raise ValueError('Invalid scoring configuration')
    distance = _integer(scoring.get('maxDistance'), 'maxDistance')
    threshold = scoring.get('scoreThreshold')
    if (distance > 4 or type(threshold) not in (int, float)
            or not math.isfinite(threshold) or not 0 <= threshold <= 100
            or scoring.get('scoreMethod') not in ('mit', 'cfd', 'and', 'or', 'avg')):
        raise ValueError('Invalid scoring configuration')


def _parse_task(record):
    task = json.loads(record['body'])
    if task.get('taskType') == 'mapperReference':
        if task.get('schemaVersion') != 1:
            raise ValueError('Unsupported Mapper reference schemaVersion')
        batch = _read_json(task['bucket'], task['batchKey'])
        index = _integer(task.get('mapperTaskIndex'), 'mapperTaskIndex')
        if index >= len(batch['mapperTasks']):
            raise ValueError('Invalid mapper task index')
        task = batch['mapperTasks'][index]
    if task.get('schemaVersion') != 7 or task.get('taskType') != 'mapperBatch':
        raise ValueError('Unsupported or missing Mapper task schemaVersion/taskType')
    for name in ('jobId', 'batchId', 'genome'):
        _string(task.get(name), name)
    id_bits = _integer(task.get('idBits'), 'idBits')
    if id_bits not in (32, 64) or task.get('hydratedRecordBytes') != 12 + id_bits // 8:
        raise ValueError('Mapper task hydrated-record width does not match ID width')
    _validate_scoring(task.get('scoring'))
    for name in ('bucket', 'metadataKey'):
        _string(task.get('output', {}).get(name), f'output.{name}')
    if 'plannedHydratedBytes' in task:
        planned = _integer(task['plannedHydratedBytes'], 'plannedHydratedBytes')
        if planned > MAPPER_SAFE_BYTES:
            raise ValueError('Planned hydrated data exceeds Mapper safe input limit')
    buckets = task.get('buckets')
    if not isinstance(buckets, list) or not buckets:
        raise ValueError('Mapper task requires selected buckets')
    identities, manifest_keys, slice_order = set(), set(), []
    for bucket in buckets:
        slice_id = _integer(bucket.get('sliceId'), 'sliceId')
        bucket_id = _integer(bucket.get('bucketId'), 'bucketId')
        key = _string(bucket.get('manifestKey'), 'manifestKey')
        if (slice_id, bucket_id) in identities or key in manifest_keys:
            raise ValueError('Duplicate selected bucket or manifest')
        identities.add((slice_id, bucket_id))
        manifest_keys.add(key)
        if not slice_order or slice_order[-1] != slice_id:
            if slice_id in slice_order:
                raise ValueError('Task bucket list must preserve index slice order')
            slice_order.append(slice_id)
    guides = task.get('guides')
    if not isinstance(guides, list) or not guides:
        raise ValueError('Mapper task must contain at least one guide')
    target_ids, selected = set(), set()
    for guide in guides:
        target_id = _integer(guide.get('targetId'), 'targetId')
        sequence = guide.get('guideSequence')
        if (not isinstance(sequence, str) or len(sequence) != 20
                or any(base not in 'ACGT' for base in sequence)):
            raise ValueError('Each Mapper guide must contain exactly 20 A/C/G/T bases')
        if target_id in target_ids:
            raise ValueError('Duplicate target ID in Mapper batch')
        target_ids.add(target_id)
        indexes = guide.get('bucketIndexes')
        if not isinstance(indexes, list) or len(indexes) != len(slice_order):
            raise ValueError('Each guide requires one selected bucket per slice')
        for index in indexes:
            _integer(index, 'bucketIndex')
            if index >= len(buckets):
                raise ValueError('Guide bucket index is out of range')
        if [buckets[index]['sliceId'] for index in indexes] != slice_order:
            raise ValueError('Guide bucket indexes must select one bucket per slice in order')
        selected.update(indexes)
    if selected != set(range(len(buckets))):
        raise ValueError('Mapper task contains unselected buckets')
    return task


def _task_fingerprint(task):
    return hashlib.sha256(json.dumps(
        task, sort_keys=True, separators=(',', ':'), allow_nan=False,
    ).encode()).hexdigest()


def _validate_scores(task, scores):
    if not isinstance(scores, list) or len(scores) != len(task['guides']):
        raise ValueError('Mapper score row count does not match guides')
    method = task['scoring']['scoreMethod']
    for guide, row in zip(task['guides'], scores):
        if type(row.get('targetId')) is not int or row['targetId'] != guide['targetId']:
            raise ValueError('Mapper score target order does not match guides')
        for name in ('mit', 'cfd'):
            value = row.get(name)
            enabled = method != ('cfd' if name == 'mit' else 'mit')
            if (type(value) not in (int, float) or not math.isfinite(value)
                    or (enabled and not 0 <= value <= 100)
                    or (not enabled and value != -1)):
                raise ValueError(f'Invalid Mapper {name} score for method {method}')


def _already_complete(task):
    try:
        marker = _read_json(task['output']['bucket'], task['output']['metadataKey'])
    except ClientError as error:
        if error.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey', 'NotFound'):
            return False
        raise
    semantics = 'complete' if task['scoring']['scoreThreshold'] == 0 else 'thresholded'
    if (marker.get('schemaVersion') != 7 or marker.get('status') != 'complete'
            or any(marker.get(key) != task[key] for key in ('jobId', 'batchId', 'genome', 'scoring'))
            or marker.get('scoreSemantics') != semantics
            or marker.get('taskFingerprint') != _task_fingerprint(task)):
        raise ValueError('Existing Mapper marker does not match this task')
    _validate_scores(task, marker.get('scores'))
    return True


def _bucket_manifest(task, bucket):
    manifest = _read_json(task['output']['bucket'], bucket['manifestKey'])
    if (manifest.get('schemaVersion') != 2
            or manifest.get('sliceId') != bucket['sliceId']
            or manifest.get('bucketId') != bucket['bucketId']):
        raise ValueError('Invalid hydrated bucket manifest identity')
    fmt = manifest.get('recordFormat', {})
    if (manifest.get('idBits') != task['idBits']
            or fmt.get('endianness') != 'little' or fmt.get('packing') != 'packed'
            or fmt.get('recordBytes') != task['hydratedRecordBytes']):
        raise ValueError('Hydrated bucket width/format does not match Mapper task')
    count = _integer(manifest.get('recordCount'), 'manifest recordCount')
    parts = manifest.get('parts')
    if not isinstance(parts, list) or not parts:
        raise ValueError('Hydrated manifest requires catalogue partition parts')
    for part in parts:
        _integer(part.get('startId'), 'fragment startId')
    parts = sorted(parts, key=lambda part: part['startId'])
    next_id, total, keys = 0, 0, set()
    for part in parts:
        start = part['startId']
        end = _integer(part.get('endId'), 'fragment endId')
        records = _integer(part.get('recordCount'), 'fragment recordCount')
        if (start != next_id or end < start or records > end - start
                or part.get('sliceId') != bucket['sliceId']
                or part.get('bucketId') != bucket['bucketId']
                or part.get('idBits') != task['idBits']
                or part.get('recordBytes') != task['hydratedRecordBytes']):
            raise ValueError('Hydrated fragment range/identity/width is invalid')
        if records:
            key = _string(part.get('key'), 'fragment key')
            if key in keys:
                raise ValueError('Repeated hydrated fragment key')
            keys.add(key)
        total += records
        next_id = end
    if total != count:
        raise ValueError('Hydrated candidate count does not match manifest')
    return {**manifest, 'parts': parts}


def _download_part(bucket, key, output, expected):
    body = s3_client.get_object(Bucket=bucket, Key=key)['Body']
    copied = 0
    try:
        while copied < expected:
            chunk = body.read(min(COPY_CHUNK_BYTES, expected - copied))
            if not chunk:
                break
            output.write(chunk)
            copied += len(chunk)
        if copied != expected or body.read(1):
            raise ValueError('Hydrated candidate part bytes do not match manifest')
    finally:
        body.close()


def _materialize_candidates(task, directory):
    # Four downloads use bounded buffers. Bucket files preserve task order
    # regardless of completion order, and are deleted as they are concatenated.
    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
        manifests = list(pool.map(lambda bucket: _bucket_manifest(task, bucket), task['buckets']))
        total_bytes = sum(m['recordCount'] * task['hydratedRecordBytes'] for m in manifests)
        if total_bytes > MAPPER_SAFE_BYTES:
            raise ValueError('Hydrated data exceeds Mapper safe input limit')
        if 'plannedHydratedBytes' in task and total_bytes != task['plannedHydratedBytes']:
            raise ValueError('Hydrated bytes do not match Dispatcher planned bytes')
        # At most two copies of the input exist during concatenation.
        if shutil.disk_usage(directory).free < 2 * total_bytes + COPY_CHUNK_BYTES:
            raise ValueError('Insufficient local staging disk for hydrated candidates')

        def download(index_manifest):
            index, manifest = index_manifest
            path = os.path.join(directory, f'bucket-{index}.bin')
            with open(path, 'wb') as output:
                for part in manifest['parts']:
                    if part['recordCount']:
                        _download_part(task['output']['bucket'], part['key'], output,
                                       part['recordCount'] * task['hydratedRecordBytes'])
            return path

        paths = list(pool.map(download, enumerate(manifests)))
    plans = []
    path = os.path.join(directory, 'candidates.bin')
    with open(path, 'wb') as output:
        for source, manifest in zip(paths, manifests):
            plans.append({'compactOffset': output.tell(), 'elementCount': manifest['recordCount']})
            with open(source, 'rb') as fragment:
                shutil.copyfileobj(fragment, output, COPY_CHUNK_BYTES)
            os.remove(source)
    return path, plans, sum(len(m['parts']) for m in manifests)


def _write_mapper_inputs(task, guides, bucket_plans, directory):
    query_path = os.path.join(directory, 'query.txt')
    plan_path = os.path.join(directory, 'bucket-plan.txt')
    with open(query_path, 'w', newline='\n') as output:
        output.write(''.join(f'{g["guideSequence"]}\n' for g in guides))
    with open(plan_path, 'w', newline='\n') as output:
        output.write(f'{len(bucket_plans)}\n')
        for bucket in bucket_plans:
            output.write(f'{bucket["compactOffset"]} {bucket["elementCount"]}\n')
        output.write(f'{len(guides)}\n')
        for guide in guides:
            indexes = guide['bucketIndexes']
            output.write(f'{len(indexes)} {" ".join(map(str, indexes))}\n')
    return query_path, plan_path


def _run_mapper(task, directory, query_path, plan_path, timeout=None):
    scoring = task['scoring']
    command = [MAPPER_BINARY, os.path.join(directory, 'candidates.bin'), query_path,
               plan_path, str(task['idBits']), str(scoring['maxDistance']),
               str(scoring['scoreThreshold']), scoring['scoreMethod']]
    stdout_path = os.path.join(directory, 'scores.tsv')
    stderr_path = os.path.join(directory, 'native.stderr')
    with open(stdout_path, 'wb') as stdout, open(stderr_path, 'wb') as stderr:
        completed = subprocess.run(command, cwd=directory, check=False,
                                   stdout=stdout, stderr=stderr, timeout=timeout)
    if completed.returncode != 0:
        with open(stderr_path, 'rb') as stderr:
            stderr.seek(max(0, os.path.getsize(stderr_path) - 8192))
            message = stderr.read(8192).decode('utf-8', errors='replace')
        raise RuntimeError(f'Mapper exited with {completed.returncode}: {message}')
    return stdout_path


def _parse_scores(task, path):
    scores = []
    with open(path, 'r', encoding='utf-8') as output:
        for guide in task['guides']:
            line = output.readline(1024)
            fields = line.rstrip('\r\n').split('\t')
            if len(fields) != 3 or fields[0] != guide['guideSequence']:
                raise ValueError('Mapper score sequence/order or TSV row is invalid')
            try:
                scores.append({'targetId': guide['targetId'],
                               'mit': float(fields[1]), 'cfd': float(fields[2])})
            except ValueError as error:
                raise ValueError('Mapper produced a nonnumeric score') from error
        if output.read(1):
            raise ValueError('Mapper produced extra score rows')
    _validate_scores(task, scores)
    return scores


def _process(task, context=None):
    if _already_complete(task):
        print(json.dumps({'event': 'mapper_batch_already_complete', 'batchId': task['batchId']}))
        return
    if not os.path.exists(MAPPER_BINARY):
        shutil.copyfile(MAPPER_BINARY_SOURCE, MAPPER_BINARY)
        os.chmod(MAPPER_BINARY, 0o755)
    with tempfile.TemporaryDirectory() as directory:
        path, plans, fragment_count = _materialize_candidates(task, directory)
        query, plan = _write_mapper_inputs(task, task['guides'], plans, directory)
        timeout = None
        if context is not None and hasattr(context, 'get_remaining_time_in_millis'):
            timeout = context.get_remaining_time_in_millis() / 1000 - 15
            if timeout <= 0:
                raise TimeoutError('Insufficient Lambda time remaining to start scoring')
        scores = _parse_scores(task, _run_mapper(task, directory, query, plan, timeout))
        marker = {
            'schemaVersion': 7, 'status': 'complete',
            **{name: task[name] for name in ('jobId', 'batchId', 'genome', 'scoring')},
            'scoreSemantics': 'complete' if task['scoring']['scoreThreshold'] == 0 else 'thresholded',
            'taskFingerprint': _task_fingerprint(task), 'scores': scores,
            'materializedBytes': os.path.getsize(path), 'fragmentCount': fragment_count,
        }
        s3_client.put_object(Bucket=task['output']['bucket'], Key=task['output']['metadataKey'],
                             Body=json.dumps(marker, separators=(',', ':'), allow_nan=False).encode(),
                             ContentType='application/json')
    print(json.dumps({'event': 'mapper_batch_complete', 'batchId': task['batchId'],
                      'jobId': task['jobId'], 'guideCount': len(task['guides']),
                      'materializedBytes': marker['materializedBytes']}))


def lambda_handler(event, context):
    for record in event.get('Records', []):
        _process(_parse_task(record), context)
    return {'processed': len(event.get('Records', []))}
