# Layer: `isslCandidateExtractor`

This layer contains the precompiled Linux x86_64 candidate Extractor used by
the Crackling AWS candidate-extraction pipeline. Each native invocation hydrates
a batch of compressed ISSL buckets for one shared, contiguous, half-open global
off-target ID interval. The Extractor combines bucket IDs with signature bytes,
compressed occurrence bytes, and the applicable overflow rows downloaded by the
Lambda wrapper.

The native source is maintained in this repository at
[`native/isslCandidateExtractor/isslExtractCandidates.cpp`](../../native/isslCandidateExtractor/isslExtractCandidates.cpp).
The Python Lambda wrapper is in `modules/isslCandidateExtractor/`.
Build the source in the Lambda-compatible Python 3.10 Amazon Linux 2
container. Run the following from the Crackling-AWS repository root in a
Linux shell (for example, WSL) with Docker available:

```bash
  sudo docker run --rm \
      --platform linux/amd64 \
      --entrypoint /bin/bash \
      -v "$PWD:/src" \
      -w /src \
      public.ecr.aws/lambda/python:3.10 \
      -lc '
        yum install -y gcc-c++ &&
        g++ -o layers/isslCandidateExtractor/extractor \
          native/isslCandidateExtractor/isslExtractCandidates.cpp \
          -O3 \
          -std=c++11 \
          -static-libgcc \
          -static-libstdc++ &&
        chmod 755 layers/isslCandidateExtractor/extractor &&
        ldd layers/isslCandidateExtractor/extractor
      '
```

This command replaces `layers/isslCandidateExtractor/extractor` with the rebuilt
binary and checks its dynamic dependencies inside the same Lambda container.
The layer packages that binary at `/opt/extractor`; the Lambda wrapper copies
it to `/tmp/extractor` before execution. Rebuild the binary after changing the
C++ source and before packaging the layer.

The current Extractor resolves only Amazon Linux core libraries (`libm` and
`libc`), so it does not require additional shared libraries in `layers/lib`.

## Compressed input and hydration contract

The ISSL global catalogue layout is, in order:

```text
uint64 N
uint64 sequenceLength
uint64 sliceCount
N * 5 signature bytes
N * 1 occurrence bytes
uint64 overflowCount
overflowCount * 16 overflow rows
sliceCount * uint64 masks
per slice:
  B * uint64 decoded bucket-entry counts
  B * uint64 compressed bucket-byte counts
  uint64 total compressed payload bytes
  compressed bucket streams in bucket order
```

`sequenceLength` must be 20. Each mask is a nonzero 20-bit position mask;
`B = 1 << (2 * popcount(mask))` is calculated independently for each slice.
The Coordinator reads the declared slice count and every mask from the file,
so the format supports varying mask counts and weights, including 15 slices.
Its `NUM_SHARDS=5` environment setting is unused. The current index is built
outside this repository; the legacy `isslCreation` module and its binary do
not define this format. The
[Coordinator reader](../../modules/isslCoordinator/lambda_function.py)
defines the consumer contract.

The Dispatcher selects an exact matching bucket from each mask. Complete
retrieval within distance four requires every set of up to four mismatch
positions to be disjoint from at least one mask. The code validates the index
structure but does not verify that coverage property; the number of masks
alone does not guarantee it.

An occurrence byte in the range 1–254 is the complete count. Sentinel `255`
means the full `uint32` count is stored in a sorted 16-byte overflow row:

```text
uint64 globalId
uint32 occurrences
4 padding bytes
```

The Lambda wrapper downloads only the signature and occurrence ranges for its
ID partition. It binary-searches the sorted overflow table by global ID and
downloads only rows in that partition. For `P = endId - startId`, the staged
`signatures.bin` contains exactly `5 * P` bytes and `occurrences.bin` contains
exactly `P` bytes. Both are headerless and ordered by global ID. `overflow.bin`
contains only the partition's sorted 16-byte rows, one per sentinel occurrence;
it is empty when no counts overflow. Zero occurrence counts are invalid.

The wrapper stages every selected bucket, writes a plan, and invokes the native
Extractor once with eight positional arguments:

```text
extractor signatures.bin occurrences.bin overflow.bin buckets.tsv \
  startId endId idBits totalCount
```

All numeric CLI arguments are unsigned decimal integers, with
`0 <= startId <= endId <= totalCount <= 2^40`. `totalCount` is the full
catalogue's off-target count. `idBits` must be 32 for `totalCount <= 2^32` and
64 otherwise.

`buckets.tsv` is a nonempty, headerless, LF-delimited file. Each row contains
exactly three tab-separated fields:

```text
bucketPath<TAB>elementCount<TAB>outputPath
```

`<TAB>` denotes a literal tab; paths must not contain tabs or newlines.
`elementCount` is the complete bucket's decoded entry count, not the number of
IDs in this partition. Each bucket is a headerless unsigned LEB128 stream:
the first value is an absolute global ID and each subsequent value is a
positive delta. The decoded IDs are strictly increasing and below `totalCount`.

The Extractor maps the shared catalogue files once and processes plan rows in
order, mapping one bucket at a time. It creates or truncates each row's output
file, filters IDs to `[startId, endId)`, zero-extends each 5-byte signature to
`uint64`, and resolves sentinel occurrences. It stops decoding a bucket at the
first ID at or above `endId`; the unvisited suffix is not validated. A fully
scanned bucket must match both its declared entry count and byte length.
Mapper never receives the raw occurrence bytes or overflow table.

Hydrated output is headerless, little-endian, and packed:

| Global ID width | Record format | Bytes |
| --- | --- | --- |
| 32-bit | `<QII>`: signature, ID, occurrences | 16 |
| 64-bit | `<QQI>`: signature, ID, occurrences | 20 |

Each matching bucket ID produces one record in bucket order; occurrences
remain a field and never expand into repeated records. A bucket with no matching
IDs produces an empty file. Success returns exit status 0 and writes
`candidateRecords=<total across all outputs>` to stdout. Failures return status
1 with diagnostics on stderr; partial outputs may remain and the wrapper rejects
the invocation before uploading them.

The wrapper accepts schema-3 extraction tasks and writes a schema-2 completion
marker with each bucket's partition range, ID width, record width, and count.
Empty fragments have `key: null`. Extraction Completion verifies contiguous
partition coverage and the complete bucket count before writing schema-2
hydrated manifests. The Mapper wrapper validates manifest widths and downloaded
byte counts, then combines these hydrated records for off-target scoring.

## Working-set and output-size bound

The Lambda downloads one catalogue partition and stages all selected compressed
buckets before starting the native process. Each bucket has a distinct
`bucket-N.bin` input and `candidates-N.bin` output. Although the native process
maps buckets sequentially, all staged inputs and completed outputs remain on
disk until the task's temporary directory is cleaned up.

For capacity planning, let:

- `P = ceil(offtargetsCount / extractorCount)`, the largest ID partition;
- `K`, the complete overflow-row count;
- `B_i`, compressed bucket `i`'s size in bytes;
- `E_i`, bucket `i`'s decoded entry count; and
- `R`, the hydrated record width (16 or 20 bytes).

The raw catalogue download is bounded by:

```text
6 * P + 16 * min(K, P)
```

For all selected uncached buckets, the conservative staged-file estimate is:

```text
6 * P + 16 * min(K, P) + sum_i(B_i + min(E_i, P) * R)
```

The Dispatcher initially chooses the partition count using the largest
individual bucket footprint, then checks the cumulative footprint of the
selected uncached buckets. Guide grouping also bounds cumulative bucket work.
The Lambda logs actual catalogue, bucket, hydrated-output, and combined local
file byte counts so the estimate can be compared with production behaviour.

The feasibility check distinguishes two cases:

- `RETRY_WITH_MORE_EXTRACTORS`: the staged bucket footprint alone is below the
  configured safe working limit, but adding the catalogue partition reaches or
  exceeds that limit.
- `TOTAL_BUCKET_EXCEEDS_LAMBDA_LIMIT`: the sum of compressed bucket bytes and
  estimated partition outputs alone reaches the configured safe working limit
  at the selected partition count. The Dispatcher rejects this task; this does
  not prove that every larger partition count would also fail.

These labels describe raised errors. The current Dispatcher does not
automatically increase the partition count after the cumulative check fails.
CDK sets `MAX_EXTRACTORS=50` and `EXTRACTOR_SAFE_BYTES=8 GiB`; the estimate must
be strictly below that limit. It sets the extraction guide-group cap to 1,000
(the Python module default is 100). Every selected uncached bucket across all
slices contributes to the estimate. A single guide is kept whole even when
it exceeds the grouping budget, then accepted or rejected by the feasibility
check. The extractor-count cap limits ID partitions per group, not reserved
Lambda concurrency.

The Dispatcher can calculate this before publishing extraction work because
`shards.json` contains `offtargetsCount`, the overflow count, and every
bucket's byte boundaries and decoded-entry counts. This model primarily
protects Lambda `/tmp`. The configured safe limit must leave headroom for the
plan and other temporary files. Each merged S3 range read is bounded by
`MAX_MERGE_RANGE_BYTES` (default 16 MiB); the default merge gap is 256 KiB.
Larger buckets stream directly to disk. Memory-map residency, range
buffers, and native process overhead require separate memory headroom; the
staged-file estimate is not a peak-RAM bound. CDK configures each Extractor
with 10,240 MiB RAM, 10 GiB temporary disk and a 15-minute timeout.

## Cache and index replacement

Hydrated parts and manifests use
`<genome>/issl/cache/slices/<slice-id>/buckets/<bucket-id>/` and are tagged
`crackling-cache=extracted-candidates`. CDK expires tagged objects
asynchronously after `candidateRetentionDays` (default seven days).

Cache identities do not include an index version or mask value. Before
replacing an index under the same genome prefix, finish outstanding work
against the old index. Clear its hydrated cache and regenerate `shards.json`
through the Coordinator before new scoring work. A separate genome prefix
avoids reusing those cache identities.
