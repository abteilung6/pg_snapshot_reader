# Postgres Snapshot and CDC Reader

## Purpose

This project starts as a single Rust data-plane implementation of a Postgres-to-analytics replication pipeline.

The goal is to understand and build the core pieces behind systems such as PeerDB and other Postgres-to-ClickHouse replication tools: schema discovery, initial snapshotting, checkpointed backfills, logical replication, staging, and append-oriented analytical writes.

This project is primarily a correctness exercise before it is a performance exercise.

This is not intended to become a generic ETL framework. The design is intentionally narrow: Postgres as the source, a batch-oriented intermediate representation, and eventually a ClickHouse-style append ingestion model as the target.

## Design Direction

The project follows the basic separation used by production CDC systems:

```text
Postgres
  -> schema discovery
  -> initial snapshot
  -> checkpointing
  -> logical replication
  -> staging
  -> target writer
```

The main correctness problem is the handoff between the initial snapshot and the CDC stream. A correct system must avoid missing rows and avoid duplicating rows when switching from backfill to WAL-based replication.

## Core Invariant

The system is only correct if the initial snapshot and the CDC stream form one continuous history of the source table.

For every replicated table, the pipeline must eventually guarantee:

```text
all rows visible in the snapshot are copied
all committed changes after the snapshot boundary are captured
no committed change is skipped
retries do not create incorrect duplicates in the target model
source WAL is acknowledged only after the change is durably staged or written
```

This invariant is more important than throughput. Performance optimizations are only valid if they preserve the snapshot-to-CDC handoff semantics.

## Control Plane and Data Plane

This project starts with the data plane only.

The data plane is responsible for moving data correctly:

```text
schema discovery
snapshot reading
checkpointing
logical replication
staging
target writes
```

A control plane would be responsible for operating replication jobs:

```text
job configuration
job lifecycle
API or CLI
scheduling
worker coordination
status reporting
multi-tenant state
```

For the initial version, both concerns can live in one Rust binary. This keeps the system small and makes the data movement path explicit.

A separate control plane can be added later once the data plane is correct. That control plane could be written in Rust or another service-oriented language such as Go. The important boundary is not the language split, but the responsibility split:

```text
control plane -> decides what should run
data plane    -> moves data and preserves correctness
```

The project intentionally starts with the data plane because snapshot correctness, checkpointing, WAL consumption, and target write semantics are the hard parts to understand first.

## Related Systems and Trade-offs

### PeerDB

PeerDB is a purpose-built Postgres CDC system with strong focus on Postgres-to-ClickHouse replication. It uses Postgres logical replication for CDC and, for ClickHouse peers, uses an intermediary S3 stage under the hood for performance. This decouples reading from Postgres from writing into ClickHouse, which helps keep the replication slot moving even when the target side is slower or temporarily unavailable.

The PeerDB-style design is attractive because it treats Postgres and ClickHouse as different systems with different strengths:

```text
Postgres     -> OLTP source of truth
CDC reader   -> ordered change extraction
Stage        -> buffering and retry boundary
ClickHouse   -> append-oriented analytical target
```

The trade-off is that this architecture introduces more internal state. The system needs to track snapshot progress, WAL LSNs, staged batches, target write state, and schema changes.

This project follows PeerDB’s narrow-source, analytics-target direction, but starts with a smaller scope: local Postgres, local batch representation, and no distributed staging layer at first.

### Altinity Sink Connector

The Altinity Sink Connector also targets transactional databases such as MySQL and PostgreSQL and replicates them into ClickHouse. Altinity describes the newer connector as a single executable, avoiding the operational overhead of Kafka Connect-based deployments while still supporting initial loading and streaming replication.

This design has a different operational trade-off:

```text
Single process
  -> simpler deployment
  -> fewer moving parts
  -> tighter coupling between source reading and target writing
```

Compared with a staged architecture, a single-process connector can be easier to run and reason about, but it needs careful backpressure handling. If the target becomes slow, the source-side replication process must avoid falling behind and retaining too much WAL.

This project borrows from that simplicity early on: one Rust process, explicit state, local integration tests, and no external queue. A staging layer can be added later once the source and snapshot semantics are clear.

### Kafka / Debezium / Sink Connector Pipelines

A common alternative is:

```text
Postgres
  -> Debezium
  -> Kafka or Redpanda
  -> Schema Registry
  -> ClickHouse Sink Connector
```

This is flexible and battle-tested, especially in organizations that already operate Kafka. The trade-off is operational complexity: more services, more failure modes, more configuration, and more latency boundaries.

This project intentionally avoids that model. The goal is not to build a generic event streaming platform. The goal is to understand the direct Postgres-to-analytics replication path.

## Core Components

### Schema Discovery

Reads table metadata from Postgres.

Responsibilities:

```text
discover column names
discover Postgres types
detect nullable columns
detect primary key columns
provide schema input for snapshot queries
```

### Snapshot Reader

Reads a table in primary-key order using keyset pagination.

Example query shape:

```sql
SELECT <columns>
FROM <table>
WHERE <primary_key> > $1
ORDER BY <primary_key>
LIMIT $2;
```

Responsibilities:

```text
read rows in batches
avoid OFFSET-based pagination
return generic SnapshotRow values
track the last processed primary key
```

### Checkpoint Store

Persists snapshot progress.

Responsibilities:

```text
store last processed primary key
resume interrupted backfills
separate reader state from row data
prepare state handoff into CDC
```

A first implementation can use a local JSON file.

### Consistent Snapshot

Provides a stable MVCC view of the source table.

Responsibilities:

```text
read from a consistent transaction
capture a snapshot boundary
coordinate initial backfill with later WAL streaming
```

This is required before the snapshot reader can be safely combined with logical replication.

### Logical Replication Reader

Consumes changes from Postgres WAL through logical decoding.

Responsibilities:

```text
create or use a replication slot
consume INSERT, UPDATE, DELETE events
track LSN progress
acknowledge consumed WAL positions
```

This is the CDC part of the system.

### Stage Writer

Decouples source reading from target writing.

Responsibilities:

```text
persist snapshot and CDC batches
allow retrying target writes
reduce pressure on the replication slot
provide a stable handoff format
```

A first implementation can write local JSONL files. Later versions could write to S3 or another object store.

### Target Writer

Writes batches into an analytical system.

Responsibilities:

```text
write rows in batches
avoid row-by-row target writes
represent updates as new versions
represent deletes as tombstone rows
support append-oriented analytical storage
```

For ClickHouse, the target model should eventually align with append-friendly engines such as `ReplacingMergeTree`.

## Milestones

### Milestone 1: Local Postgres Snapshot

```text
run Postgres locally
connect from Rust
read rows from a table
read rows in primary-key batches
test against real Postgres
```

### Milestone 2: Generic Snapshot Reader

```text
discover schema
generate SELECT dynamically
extract rows into SnapshotRow
use discovered primary key for pagination
remove hardcoded users model
```

### Milestone 3: Checkpointed Backfill

```text
store last processed primary key
resume snapshot from checkpoint
handle interrupted backfills
separate reader state from row data
```

### Milestone 4: Stable Snapshot Boundary

```text
understand MVCC snapshot consistency
read using a stable transaction
record the WAL/LSN boundary
prepare handoff from snapshot to CDC
```

### Milestone 5: Logical Replication

```text
enable logical replication
create publication
create replication slot
stream WAL events
decode INSERT, UPDATE, DELETE
track LSN acknowledgements
```

### Milestone 6: Staging Layer

```text
write snapshot batches to local JSONL
write CDC batches to local JSONL
define stable batch format
separate source reader from target writer
```

### Milestone 7: ClickHouse-Oriented Writer

```text
batch insert into ClickHouse
append updates as new versions
write deletes as tombstones
experiment with ReplacingMergeTree
```

## Scope Boundaries

The project should avoid becoming a broad data integration framework.

Not part of the initial data-plane implementation:

```text
multiple source databases
generic transformation engine
UI
job scheduler
distributed execution
production-grade observability
```

The first goal is to implement the core replication path clearly and correctly before adding operational features.