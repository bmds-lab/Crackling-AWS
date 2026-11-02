# ISSL batch result finalizer

The Reducer persists final MIT/CFD scores calculated by the combined Mapper.
It consumes an S3 notification, directly or through the existing SQS envelope,
for this schema-7 batch result:

```text
<genome>/mapper/<job-id>/batches/<batch-id>/mapper-result.json
```

Old per-guide/shard paths and unrelated objects are ignored. A result on the
batch path with an incompatible schema fails rather than mixing scoring formats.
See the [Mapper README](../../layers/isslMapper/README.md) for its scoring
interface and the [Extractor README](../../layers/isslCandidateExtractor/README.md)
for hydration and Extraction Completion contracts.

## Validation and persistence

The complete result must have matching job, batch and genome identities,
`status: "complete"`, valid scoring settings and a nonempty score list with
unique nonnegative Target IDs. Enabled scores must be finite within 0..100;
disabled MIT/CFD scores must be `-1`. `scoreSemantics` is `complete` at threshold
zero and `thresholded` at a positive threshold. Every row is validated before
any target is written. Thresholded values can be early-stop scores.

For each new Mapper batch, one DynamoDB transaction atomically:

- stores `IsslScore`, `CfdScore` and `ReducerTaskId` on every existing target;
- increments `TaskTracking.NumScoredOfftarget` by the batch's guide count and increments `Version` once;
- inserts a conditional completion record in `ddbIsslBatchCompletions`.

New Mapper batches are configured at 95 guides: 95 target updates, one tracking
update and one completion record use 97 of DynamoDB's 100 transaction actions.
The Reducer enforces a hard maximum of 98 guides.
Completion records are keyed by `JobID` and the Mapper `BatchID`, and contain
the source digest and guide count. The durable
record prevents double counting even if S3 publication fails or the transaction
commits but its response is lost. All target conditions are checked before any
new scores or progress are committed.

Each target's `ReducerTaskId` is `<job-id>:<target-id>`; the durable batch guard
is keyed separately by `JobID` and `BatchID`. Conditional guards
require an existing target without `ReducerTaskId` and an existing tracking
item. Transaction conflicts and throughput throttling use 16 attempts with exponential jitter starting
at 0.1 seconds and capped at 2 seconds per sleep. Retries stop early if the
Lambda context has insufficient remaining time.
Retryable cancellations include mixtures of `TransactionConflict`,
`ThrottlingError` and `ProvisionedThroughputExceeded`. Top-level throttling
exceptions also retry after SDK retries are exhausted. Conditional/validation
failures do not retry unless the durable guard proves this batch committed.
Throttling logs use `reducer_transaction_throttled` and include error/reason
details. Cancellation retries do not add guard reads while the service is
throttling; the conditional transaction still prevents duplicate completion.
On transaction cancellation, a consistent completion-record read recognizes
a duplicate committed batch. Other conditional failures remain errors. Results
above 98 guides are rejected before writes; there is no recovery of old
per-guide completions or splitting of old batches. A completed target without
the matching batch guard causes the entire transaction to fail.

The Reducer performs no candidate downloads, off-target deduplication,
contribution summation or SQLite reduction. CDK configures 4,096 MB memory
and default ephemeral storage for this finalizer.

## Completion and retries

After the batch transaction commits or its durable completion record confirms
an earlier commit, the Reducer writes one final marker:

```text
<genome>/reducer/<job-id>/batches/<batch-id>/result.json
```

It includes schema/status, job/batch/genome identities, `sourceMapperKey`,
`guideCount`, scoring settings, score semantics and `sourceDigest`, the SHA256
of the canonical Mapper result JSON. A matching final marker skips further
DynamoDB work; changed source content or a conflicting marker fails.

A failed batch transaction writes no targets or progress. A failure after it
commits leaves its durable completion record; retries skip the committed batch
without advancing counters again. SQS delivery returns `batchItemFailures` for failed messages while
allowing other messages to succeed; direct S3 failures propagate.

CDK creates the completion table, sets `BATCH_COMPLETIONS_TABLE`, grants table
access plus `TransactWriteItems` for all three tables, and configures the SQS
event source to report batch-item failures. Its batch size is 10, the Lambda
timeout is 15 minutes, reserved concurrency is 50, and the SQS event source
has a maximum concurrency of 10. The reserved concurrency limit belongs to
the Reducer; the Dispatcher's `MAX_EXTRACTORS=50` independently limits
extraction ID partitions per group.
