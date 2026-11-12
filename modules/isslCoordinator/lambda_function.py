import json
import os
import struct

import boto3


ISSL_HEADER_FIELDS = 6
SIZE_T_BYTES = 8
UINT64_BYTES = 8
DOUBLE_BYTES = 8
ISSL_HEADER_BYTES = ISSL_HEADER_FIELDS * SIZE_T_BYTES

BUCKET = os.environ['BUCKET']
TARGET_SCAN_QUEUE_URL = os.environ['TARGET_SCAN_QUEUE']
NUM_SHARDS = int(os.getenv('NUM_SHARDS', '5'))
MAX_DISTANCE = int(os.getenv('MAX_DISTANCE', '4'))
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')


def _read_s3_range(key, start, length):
    if length <= 0:
        return b''

    response = s3_client.get_object(
        Bucket=BUCKET,
        Key=key,
        Range=f'bytes={start}-{start + length - 1}',
    )
    data = response['Body'].read()
    if len(data) != length:
        raise ValueError(
            f'Expected {length} bytes from s3://{BUCKET}/{key} at offset '
            f'{start}, received {len(data)}'
        )
    return data


def _read_issl_layout(key, file_bytes):
    """
    Read the compressed matching-format ISSL index from S3.
    Returns:
        layout:
            Metadata required to download catalogue partitions and interpret
            hydrated candidate records. This includes the global off-target
            count, signature/occurrence offsets, overflow-table location, and
            the selected 32- or 64-bit global-ID contract.

        shards:
            One dictionary per ISSL slice. Each shard contains its mask,
            decoded bucket-entry counts, and absolute S3 byte offsets for the
            compressed LEB128 bucket streams.

    Layout, little-endian:

        3 x uint64
            N, sequence length, and slice count.

        N x 5-byte signatures
            One packed 40-bit signature per unique off-target.

        N x uint8 occurrence bytes
            Counts 1-254 are stored directly. A value of 255 indicates that
            the full uint32 count is stored in the overflow table.

        uint64 overflow count

        overflow count x 16-byte entries
            Each entry is: uint64 global ID, uint32 occurrence count, and
            four bytes of x86-64 struct padding.

        slice count x uint64 masks

        Per slice:
            B x uint64 decoded bucket-entry counts
            B x uint64 compressed bucket-byte counts
            1 x uint64 total compressed payload bytes
            B contiguous LEB128 delta-encoded global-ID streams

    B is calculated independently for each slice:

        B = 1 << (2 * popcount(mask))

    Each bucket begins with an absolute global ID. Later LEB128 values are
    positive deltas from the previous ID. The resulting IDs index the
    signature and occurrence arrays. This function validates the declared
    byte ranges and returns absolute offsets suitable for S3 range requests.
    """
    def checked_span(start, length):
        if start < 0 or length < 0 or start > file_bytes - length:
            raise ValueError('Index region exceeds object size')

    def words(start, count):
        checked_span(start, count * 8)
        data = _read_s3_range(key, start, count * 8)
        return struct.unpack(f'<{count}Q', data)

    n, sequence_length, slice_count = words(0, 3)
    contract = _id_contract(n)
    if sequence_length != 20:
        raise ValueError('Expected a 20-base matching-format index')
    if slice_count == 0:
        raise ValueError('Invalid slice count')

    signature_offset = 24
    occurrence_offset = signature_offset + 5 * n
    overflow_count_offset = occurrence_offset + n
    overflow_count, = words(overflow_count_offset, 1)
    if overflow_count > n:
        raise ValueError('More overflow entries than catalogue records')
    overflow_offset = overflow_count_offset + 8
    checked_span(overflow_offset, overflow_count * 16)
    mask_offset = overflow_offset + overflow_count * 16
    masks = words(mask_offset, slice_count)
    cursor = mask_offset + 8 * slice_count

    shards = []
    for slice_id, mask in enumerate(masks):
        weight = int(mask).bit_count()
        if mask >> 20 or weight == 0:
            raise ValueError('Slice mask is outside the 20-base signature')
        bucket_count = 1 << (2 * weight)
        entry_counts = words(cursor, bucket_count)
        if sum(entry_counts) != n:
            raise ValueError("Slice entry counts do not match size")
        cursor += 8 * bucket_count
        byte_counts = words(cursor, bucket_count)
        if any(entry_count == 0 and byte_count != 0 for entry_count, byte_count in zip(entry_counts,byte_counts)):
            raise ValueError('Empty bucket has encoded payload bytes')
        cursor += 8 * bucket_count
        total_bytes, = words(cursor, 1)
        cursor += 8
        if sum(byte_counts) != total_bytes:
            raise ValueError('Compressed bucket lengths do not match slice total')
        offsets = [cursor]
        for length in byte_counts:
            cursor += length
            offsets.append(cursor)
        if cursor > file_bytes:
            raise ValueError('Slice payload exceeds the index object')
        shards.append({
            'shardId': slice_id,
            'sliceId': slice_id,
            'mask': mask,
            'maskWeight': weight,
            'bucketCount': bucket_count,
            'bucketEntryCounts': list(entry_counts),
            'bucketOffsets': offsets,
        })

    if cursor != file_bytes:
        raise ValueError('Unexpected trailing bytes in matching-format index')
    return {
        'format': 'delta-catalogue-v2',
        'offtargetsCount': n,
        'sequenceLength': sequence_length,
        'sliceCount': slice_count,
        'signatureOffsetBytes': signature_offset,
        'signatureRecordBytes': 5,
        'occurrenceOffsetBytes': occurrence_offset,
        'occurrenceRecordBytes': 1,
        'overflowOffsetBytes': overflow_offset,
        'overflowCount': overflow_count,
        'overflowRecordBytes': 16,
        **contract,
    }, shards

def _id_contract(offtarget_count):
    count = int(offtarget_count)
    if not 0 <= count <= (1 << 40):
        raise ValueError('Invalid unique 20-base off-target count')
    if count > (1 << 32):
        return {
            'idBits': 64,
            'hydratedRecordBytes': 20,
            'hydratedStruct': '<QQI',
            'scoreRecordBytes': 16,
            'scoreStruct': '<Qd',
        }
    return {
        'idBits': 32,
        'hydratedRecordBytes': 16,
        'hydratedStruct': '<QII',
        'scoreRecordBytes': 12,
        'scoreStruct': '<Id',
    }

def _parse_record(record):
    message = json.loads(record['body'])
    if message.get('schemaVersion') != 1:
        raise ValueError('Unsupported or missing Coordinator message schemaVersion')

    for field in ('JobID', 'Genome', 'Sequence'):
        if field not in message:
            raise ValueError(f'Coordinator message is missing {field}')
    return message


def _process_job(message):
    job_id = str(message['JobID'])
    genome = str(message['Genome'])
    issl_key = f'{genome}/issl/{genome}.issl'
    output_prefix = f'{genome}/coordinator/{job_id}'

    file_bytes = s3_client.head_object(Bucket=BUCKET, Key=issl_key)['ContentLength']
    layout, shards = _read_issl_layout(issl_key, file_bytes)
    audit_document = {
        'schemaVersion': 4,
        'jobId': job_id,
        'genome': genome,
        'maxDistance': MAX_DISTANCE,
        'issl': {'bucket': BUCKET, 'key': issl_key},
        'layout': {
            key: value
            for key, value in layout.items()
            if key != 'slicelistSizes'
        },
        'shards': shards,
    }
    audit_key = f'{genome}/issl/shards.json'
    s3_client.put_object(
        Bucket=BUCKET,
        Key=audit_key,
        Body=json.dumps(audit_document, indent=2).encode('utf-8'),
        ContentType='application/json',
    )

    print(json.dumps({
        'event': 'coordinator_shards_calculated',
        'jobId': job_id,
        'genome': genome,
        'isslKey': issl_key,
        'auditKey': audit_key,
        'shards': shards,
    }))

    # Target Scan is released only after the complete shard manifest exists.
    # Its algorithm and input contract remain unchanged.
    response = sqs_client.send_message(
        QueueUrl=TARGET_SCAN_QUEUE_URL,
        MessageBody=json.dumps({
            'Genome': genome,
            'Sequence': message['Sequence'],
            'JobID': job_id,
        }),
    )

    print(json.dumps({
        'event': 'target_scan_released',
        'jobId': job_id,
        'messageId': response.get('MessageId'),
        'queueUrl': TARGET_SCAN_QUEUE_URL,
    }))


def lambda_handler(event, context):
    for record in event.get('Records', []):
        _process_job(_parse_record(record))

    return {'processed': len(event.get('Records', []))}
