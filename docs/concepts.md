# Optimizations

This document captures optimization concepts for a Postgres-to-ClickHouse replication system.

It is intentionally not part of the first implementation path. The first goal is a small end-to-end pipeline that is understandable and correct. Once that exists, the system can be made faster without losing the shape of the problem.

The core idea is simple:

```text
make the data path explicit
make progress durable
then increase parallelism
```

## Architecture Comparison

| Architecture                | Shape                                                                    | Strength                                                | Limitation                                                                            |
| --------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Dump and restore            | `Postgres -> dump stream -> target`                                      | Simple, proven, easy to invoke                          | Coarse progress, weak retry granularity, limited single-table parallelism             |
| Native logical replication  | `Postgres publication -> subscription -> target`                         | Built into Postgres, supports online catch-up           | Initial table synchronization can still be bottlenecked by single-table sync behavior |
| Direct custom pipeline      | `Postgres -> reader -> target writer`                                    | Small deployment, easy to reason about                  | Source and target are tightly coupled; slow target can slow source progress           |
| Staged replication pipeline | `Postgres -> reader -> durable stage -> target writer`                   | Decouples source progress from target writes            | Requires batch metadata, retry state, and more internal bookkeeping                   |
| Optimized staged pipeline   | `snapshot partitions + CDC stream -> durable stage -> analytical target` | High throughput, retryable partitions, live replication | More complex correctness model                                                        |

The target direction for this project is the staged replication pipeline:

```text
Postgres
  -> schema discovery
  -> snapshot reader
  -> durable stage
  -> ClickHouse writer
```

Then, after snapshot correctness is clear:

```text
Postgres WAL
  -> logical replication reader
  -> durable stage
  -> ClickHouse writer
```

The snapshot stream and the WAL stream must eventually form one continuous history.

## Performance Is a Consequence of Boundaries

The important optimizations are not random speed tricks. They come from better boundaries.

A weak system says:

```text
I read some rows and wrote some rows.
```

A stronger system says:

```text
I copied partition 17 from snapshot S.
It produced batch B.
Batch B is durable.
Batch B was written to the target.
The source position can now advance.
```

Performance work becomes safe only when the system can name its units of progress.

The main units are:

```text
snapshot partition
snapshot batch
CDC batch
source LSN
target write
retry attempt
```

Without these units, failure recovery is guesswork.

## Initial Load Is the First Bottleneck

For large migrations, CDC is not always the first hard performance problem. The initial load can dominate total migration time.

A traditional dump-and-restore path is often sequential for a single large table. Native logical replication improves the online story, but the initial table synchronization can still be limited by single-table copy behavior.

An optimized system treats initial load as its own subsystem.

The difference is:

```text
basic snapshot:
  one table -> one ordered scan -> batches

optimized snapshot:
  one table -> many logical partitions -> many workers -> batches
```

This changes both throughput and failure recovery.

If one large table has billions of rows, the question is not only:

```text
Can we read it?
```

The question is:

```text
Can we split it into independent, retryable, observable pieces?
```

## Snapshot Partitioning

The simple version uses primary-key pagination:

```sql
SELECT <columns>
FROM <table>
WHERE <primary_key> > $1
ORDER BY <primary_key>
LIMIT $2;
```

This is a good starting point. It is deterministic, easy to checkpoint, and easy to debug.

Its limit is that it creates one logical reader per table.

A later version should support partitioned snapshot reads:

```text
table
  -> partition 1 -> worker 1
  -> partition 2 -> worker 2
  -> partition 3 -> worker 3
  -> partition N -> worker N
```

Useful partitioning strategies:

```text
primary-key min/max ranges
NTILE-based ranges
physical CTID ranges
custom predicates
```

The intended future shape:

```text
discover table size
choose partition strategy
create partition plan
run workers concurrently
write one staged batch stream per partition
track partition completion
retry failed partitions only
```

This is one of the biggest steps toward high-throughput initial load.

## Consistent Parallel Snapshot

Parallel readers are only correct if they share the same logical view of the database.

The danger:

```text
worker 1 sees the table before an update
worker 2 sees the table after an update
```

That creates a snapshot that never existed as a real database state.

The future design needs a stable MVCC snapshot:

```text
open repeatable-read transaction
export snapshot
start workers using the same snapshot
record the snapshot boundary
start CDC from the correct boundary
```

Conceptually:

```text
all snapshot workers read from time T
CDC captures changes after time T
```

This is the handoff that matters.

The snapshot itself is not enough. The boundary between snapshot and WAL is the correctness line.

## CTID-Based Partitioning

Postgres exposes `ctid`, a system column that identifies the physical location of a row version inside a table.

A CTID-based strategy partitions the table by physical layout instead of business key.

Simplified:

```text
logical partitioning:
  id 1..1,000,000
  id 1,000,001..2,000,000

physical partitioning:
  block range A
  block range B
```

The potential advantage is storage locality. Workers can read parts of the table closer to how rows are laid out on disk, which can reduce repeated scanning and improve I/O behavior.

This is not the first implementation target.

It belongs after:

```text
single-threaded snapshot works
staging works
ClickHouse writes work
consistent snapshot semantics are understood
```

CTID partitioning is a performance layer, not a correctness foundation.

## Binary and Native Transfer Formats

The learning path starts with JSONL because it is transparent:

```text
human-readable
easy to diff
easy to test
easy to replay
```

But JSON is not the final performance format.

Text formats pay several costs:

```text
encoding values as text
larger network payloads
parsing at the target
possible type ambiguity
precision pitfalls for complex values
```

A faster design reduces transformation.

For source reads, this can mean:

```text
COPY TO STDOUT
binary COPY
cursor-based streaming
```

For ClickHouse writes, this can mean:

```text
larger JSONEachRow batches
compressed HTTP inserts
RowBinary
Native format
Parquet files in a stage
```

The principle:

```text
JSON is for visibility.
Binary formats are for sustained throughput.
```

Do not switch too early. The system should first be easy to inspect.

## Durable Stage

A stage is not just a temporary file. It is a pressure boundary.

Without a stage:

```text
source reader -> target writer
```

The source can only move as fast as the target accepts writes.

With a stage:

```text
source reader -> durable stage -> target writer
```

The source reader can make progress as soon as data is safe. The target writer can fall behind, retry, or restart independently.

This matters most for CDC.

A logical replication slot retains WAL until the consumer acknowledges progress. If the target is slow and there is no durable intermediate point, source WAL can accumulate.

The rule:

```text
acknowledge source progress only after data is durable
```

Durable can mean:

```text
written to ClickHouse
written to a local durable stage
written to object storage
written to a raw target table
```

For the first implementation, local JSONL is enough. Later, the stage should gain metadata.

## Batch Metadata

A staged batch should eventually describe itself.

Minimum useful metadata:

```text
batch_id
table_name
batch_type: snapshot | cdc
row_count
created_at
stage_path
write_status
retry_count
```

Snapshot-specific metadata:

```text
partition_id
partition_start
partition_end
last_primary_key
snapshot_id
```

CDC-specific metadata:

```text
start_lsn
end_lsn
transaction_id
commit_timestamp
event_count
```

With metadata, the system can answer:

```text
Which batches are complete?
Which batches are pending target write?
Which batches failed?
Which source position is safe to acknowledge?
Which partition needs retry?
```

This is the difference between a file dump and a replication system.

## Retry Granularity

A single sequential load has poor failure geometry.

If the job fails after many hours, recovery is often coarse. Either the job restarts, or manual cleanup is needed.

Partitioned snapshotting gives better failure geometry:

```text
partition 1 completed
partition 2 completed
partition 3 failed
partition 4 completed
```

Only the failed unit needs retry.

A mature retry model should distinguish:

```text
read failure
stage write failure
target write failure
validation failure
```

Each failure has a different safe retry point.

The desired property:

```text
a transient failure should cost one batch or one partition,
not the whole migration
```

## CDC Reader

After the snapshot path works, the system needs a logical replication reader.

The CDC reader is responsible for:

```text
creating or using a replication slot
consuming WAL changes
decoding INSERT, UPDATE, DELETE
preserving transaction order
tracking LSN progress
sending standby status updates
writing CDC batches to stage
acknowledging only durable LSNs
```

The important split is:

```text
source pull
target push
```

If target push fails, source pull should not automatically stop forever. The reader should be able to continue until the durable stage or safety limits become the bottleneck.

The risk is source WAL growth. The protection is disciplined LSN acknowledgement.

## LSN Acknowledgement

CDC correctness depends on a single rule:

```text
never acknowledge an LSN before the corresponding change is durable
```

There are two safe acknowledgement models.

Direct model:

```text
read WAL event
write to ClickHouse
acknowledge LSN
```

Staged model:

```text
read WAL event
write to durable stage
acknowledge LSN
target writer consumes later
```

The staged model is more resilient, but creates more state.

It needs to track:

```text
staged LSN range
target-applied LSN range
failed batches
retries
retention policy
```

For a serious system, this state is not optional. It is the control surface of CDC.

## TOAST and Missing Values

CDC events are not always full rows.

Large Postgres values may be stored out of line. During updates, unchanged large values may not be present in the logical replication message.

A naive representation collapses three different states:

```text
value is NULL
value changed to a new value
value is unchanged but omitted
```

Those are not the same.

The CDC event model must eventually represent them separately:

```text
Changed(value)
Null
Unchanged
Unavailable
```

Without this distinction, an update can accidentally erase a large value in the target.

Possible strategies:

```text
preserve previous target value
cache previous values inside the current batch
write raw CDC records first
merge into final table with update semantics
require stronger replica identity settings
```

This is advanced CDC correctness. It should not block the first end-to-end snapshot path, but it must be remembered before claiming production-grade CDC.

## ClickHouse Target Model

ClickHouse should be treated as an analytical, append-oriented target.

The first snapshot can write ordinary rows.

CDC should not try to behave like row-by-row OLTP updates.

A better model:

```text
insert -> append row
update -> append newer version
delete -> append tombstone
```

A future table design can include internal columns:

```text
_replication_version
_replication_deleted
_replication_batch_id
_replication_synced_at
_source_lsn
```

With a `ReplacingMergeTree`-style model, the latest version can be reconstructed by primary key.

The raw write path remains append-only. The query layer decides how to read latest state.

This aligns with ClickHouse instead of fighting it.

## Observability

For large migrations, observability is not decoration. It is part of the system design.

Useful metrics:

```text
snapshot partitions total
snapshot partitions completed
snapshot rows copied
snapshot bytes copied
current source LSN
acknowledged source LSN
replication slot lag
stage bytes pending
target write latency
target rows written
failed batches
retry count
```

The key diagnostic question:

```text
where is the bottleneck?
```

Possible answers:

```text
source scan
network
stage write
target insert
ClickHouse merge pressure
CDC decoding
WAL retention
```

Without observability, performance tuning is folklore.

## Later Roadmap

After the first end-to-end example, optimizations should be layered in this order.

### 1. Larger and Safer Batches

```text
increase batch size
write batch metadata
validate row counts per batch
make stage writes atomic
avoid rewriting already-applied stage data
```

### 2. Better ClickHouse Writes

```text
use JSONEachRow consistently
use compression
avoid tiny inserts
measure insert throughput
test RowBinary or Native format
```

### 3. Stable Snapshot Boundary

```text
repeatable-read transaction
exported snapshot
snapshot boundary tracking
prepare CDC handoff
```

### 4. Partitioned Snapshot

```text
min/max ranges
NTILE ranges
CTID ranges
multiple workers
per-partition checkpoint
per-partition retry
```

### 5. CDC

```text
publication
replication slot
WAL stream
INSERT/UPDATE/DELETE decoding
LSN checkpoint
durable CDC stage
standby status updates
```

### 6. Append-Oriented CDC Target

```text
version column
delete marker
source LSN column
ReplacingMergeTree
latest-state validation
idempotent replay
```

### 7. Advanced CDC Correctness

```text
schema evolution
large values
unchanged TOAST columns
transaction ordering
crash recovery
retention policy
replication lag alerts
```
