# Layer: `isslMapper`

This layer supplies the Linux x86_64 AVX2 scorer at `/opt/mapper`. Schema-7
Mapper tasks combine the selected hydrated buckets from all index slices for
one guide batch. The Dispatcher supplies each guide's bucket indexes, so the
Mapper downloads each required bucket once and each guide scans only its own
selected ranges. The configured batch cap is 95 guides, with a separate bound on
the total hydrated input bytes.

The 95-guide cap uses 97 DynamoDB transaction actions: 95 guide updates, the
shared job counter update and durable batch-completion record, leaving three
actions spare. The Reducer's hard maximum remains 98 guides. It commits the
guide scores, progress increment and completion record atomically. Native
scoring uses the CLI documented below; changing only the guide-count cap does
not change that interface.

The packaged `mapper` binary advertises the current seven-argument selected-
bucket interface in its embedded usage text. The Mapper C++ source is not
vendored in this repository. The source is
[`ISSLScoreOfftargetsMMF_AVX2.cpp`](https://github.com/Darragh-Nugent/CracklingPlusPlus/blob/bf62b01/ISSLScoreOfftargetsMMF/ISSLScoreOfftargetsMMF_AVX2.cpp)
at commit [`bf62b01`](https://github.com/Darragh-Nugent/CracklingPlusPlus/commit/bf62b01)
on the CracklingPlusPlus `aws-avx2-mmf-scorer` branch. Rebuilding requires a
checkout of that commit implementing the CLI and output contracts below; do
not assume an older shard/contribution scorer is interchangeable.
The build recipe below expects that checkout to provide
`ISSLScoreOfftargetsMMF/ISSLScoreOfftargetsMMF_AVX2.cpp` and its CMake target.
Embedded usage text alone does not verify runtime compatibility or scoring
correctness; validate a replacement binary in the Lambda container.

Before building, change that target's CMake options from `-march=native` to
explicit Lambda-compatible instructions:

```cmake
target_compile_options(ISSLScoreOfftargetsMMF_AVX2 PRIVATE
    -O3
    -mavx2
    -mpopcnt
)
```

Run the following from the root of the checked-out CracklingPlusPlus source
tree. It builds only the AVX2 Mapper target and its `utils` and
`otScorePenalties` dependencies, then writes the deployable binary to
`bin/mapper` at that source-tree root:

```bash
sudo docker run --rm \
  --platform linux/amd64 \
  --entrypoint /bin/bash \
  -v "$PWD:/src" \
  -w /src \
  public.ecr.aws/lambda/python:3.10 \
  -lc '
    yum install -y \
      gcc10 gcc10-c++ gcc10-binutils \
      binutils make cmake3 boost-devel libgomp &&
    rm -rf build-lambda &&
    cmake3 -S . -B build-lambda \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_COMPILER=gcc10-gcc \
      -DCMAKE_CXX_COMPILER=gcc10-g++ \
      -DCMAKE_AR=/usr/bin/ar &&
    cmake3 --build build-lambda \
      --target ISSLScoreOfftargetsMMF_AVX2 \
      --parallel "$(nproc)" &&
    mkdir -p /src/bin &&
    cp build-lambda/ISSLScoreOfftargetsMMF/ISSLScoreOfftargetsMMF_AVX2 \
      /src/bin/mapper &&
    chmod 755 /src/bin/mapper
  '
```

Copy the resulting `bin/mapper` into this layer's deployment contents at
`layers/isslMapper/mapper` before packaging. The wrapper copies `/opt/mapper`
to `/tmp/mapper` and invokes it with exactly seven positional arguments:

```text
mapper candidates.bin query.txt bucket-plan.txt idBits maxDistance scoreThreshold scoreMethod
```

`candidates.bin` contains the existing packed little-endian hydrated records:
`signature:uint64`, `globalOfftargetId:uint32-or-uint64`, `occurrences:uint32`.
The record width is 16 bytes for 32-bit IDs and 20 bytes for 64-bit IDs.
`query.txt` has one 20-base A/C/G/T guide per line. The bucket plan contains
unsigned integers with this structure:

```text
bucketCount
compactOffsetBytes candidateCount          # one row per bucket
queryCount
selectedBucketCount bucketIndex...         # one row per query
```

Bucket indexes refer to the task's bucket vector; they are not off-target IDs.
The selected bucket count is variable: one bucket per index mask/slice for
each guide, including an index with 15 slices. Masks are applied by the
Dispatcher; the scorer receives only the resulting bucket indexes and
hydrated records. See the [Extractor README](../isslCandidateExtractor/README.md)
for index format, mask coverage and cache replacement requirements.

Preserve the supplied bucket order, each guide's slice order and fragment
record order. Empty buckets retain their vector entry. Off-target IDs are
deduplicated locally per guide across its selected ranges, retaining sequence
and occurrence information. Guides remain parallel through OpenMP; the wrapper
and Lambda configuration leave thread-count selection to the OpenMP runtime.

The native process writes only final score rows to stdout, in query order:

```text
guideSequence<TAB>MIT<TAB>CFD
```

Disabled score methods use `-1`; enabled scores are finite values in 0..100.
Emit enough precision to round-trip a double. Write diagnostics to stderr and
return a nonzero exit status on failure. There is no output-prefix argument.

Threshold zero disables early stopping and produces complete scores over the
selected ranges. A positive threshold enables the reference stopping rules;
those scores may represent stopping points and the wrapper labels the batch
`scoreSemantics: "thresholded"`. It validates every row before publishing one
schema-7 `mapper-result.json` for the whole batch. The Reducer persists these
final scores and progress, without contribution downloads or deduplication.

## Capacity

The Dispatcher and Mapper both use `MAPPER_SAFE_BYTES`, configured by CDK as
8 GiB. It bounds the total hydrated records in the batch's distinct selected
buckets across all slices. Each bucket shared by several guides is staged
once, but an off-target appearing in multiple slices occupies a record in
each slice's bucket before native deduplication. The Dispatcher splits guide
batches at 95 guides or the byte budget, and rejects a single guide whose
selected buckets exceed that byte budget.

CDK gives each Mapper 10,240 MiB RAM, 10 GiB temporary disk and a 15-minute
timeout. The wrapper streams downloads with `MAPPER_DOWNLOAD_WORKERS`
(default 4, range 1..16), validates manifests and planned hydrated bytes,
and requires:

```text
free temporary disk >= 2 * hydratedInputBytes + 8 MiB
```

This conservative staging check allows slightly less than 5 GiB input on the
configured disk, depending on other disk use. The Dispatcher still partitions
against 8 GiB, so batches between that practical disk limit and 8 GiB can pass
dispatch but fail staging. The check does not automatically split those
batches. The input budget is not a bound on native peak RAM.

Native stdout and stderr go to temporary files; the process timeout uses the
remaining Lambda time with a 15-second margin. Peak memory and duration must
be measured for the actual index and workload.

## Runtime shared libraries

The AVX2 Mapper dynamically links Amazon Linux 2's Boost Regex, Boost
Iostreams, and OpenMP runtime libraries. These libraries are already committed
under `layers/lib/lib/`, and CDK packages them in the shared-library layer.
They do not need to be collected again for a normal deployment.

If a rebuilt Mapper needs refreshed dependencies, use the matching Amazon
Linux 2 libraries rather than Ubuntu/WSL host libraries. From the Crackling-AWS
repository root, run:

```bash
sudo docker run --rm \
  --platform linux/amd64 \
  --entrypoint /bin/bash \
  -v "$PWD/layers/lib/lib:/out" \
  public.ecr.aws/lambda/python:3.10 \
  -lc '
    yum install -y boost-regex boost-iostreams libgomp &&
    cp -v \
      /usr/lib64/libboost_regex-mt.so.1.53.0 \
      /usr/lib64/libboost_iostreams-mt.so.1.53.0 \
      /usr/lib64/libgomp.so.1 \
      /out/
  '
```

The target directory is deployed at `/opt/lib`. The Mapper Lambda already sets
`LD_LIBRARY_PATH` to include that path. Verify the binary in the matching
container (without overriding its system-library search path):

```bash
sudo docker run --rm \
  --platform linux/amd64 \
  --entrypoint /bin/bash \
  -v "$HOME/CracklingPlusPlus/bin/mapper:/mapper:ro" \
  -v "$PWD/layers/lib/lib:/opt/lib:ro" \
  public.ecr.aws/lambda/python:3.10 \
  -lc '
    export LD_LIBRARY_PATH=/lib64:/usr/lib64:/opt/lib &&
    ldd -v /mapper | tee /tmp/mapper-ldd.txt &&
    ! grep -q "not found" /tmp/mapper-ldd.txt
  '
```

This checks against the libraries actually packaged in `layers/lib`; it does
not install Boost or OpenMP into the test container and mask a missing layer
dependency. The output must contain no `not found` entries, including missing
versioned symbols, before deployment. The Mapper function must be attached to
both the `isslMapper` binary layer and the shared-library layer.
