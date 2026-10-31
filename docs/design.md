# Design: exactly-once CDC from Postgres into Iceberg

## Problem

Replicate an OLTP Postgres database into Iceberg tables that Trino can query, within about a minute of each commit. Every committed change must show up exactly once in the final table state, schema changes must either apply on their own or stop the pipeline with a clear reason, and the tables have to stay fast to read after thousands of streaming commits. The whole thing runs from `make up` on a laptop.

Non-goals:
- Cross-table transactional reads. An order and its items are committed in one Postgres transaction but land in separate Iceberg commits, so a reader can briefly see one without the other. Final states agree; snapshots in between are per table.
- TRUNCATE and primary key type changes. Both stop the pipeline (section 4).
- Multi-region, auth, encryption. Credentials are plain env vars.
- Kubernetes. The original plan had a Helm/Terraform phase. It was cut because it could not be run and checked next to the compose stack on one laptop. On Kubernetes, the Flink operator (last-state upgrades, Kubernetes HA) replaces the controller's job supervision.

## Architecture

```
Postgres 16  (pgoutput, publication dbz_pub, slot created by Debezium)
  |  at-least-once, per-key ordered; Connect SIGKILL replays from the last flushed offset
  v
Debezium 3.7 on Kafka Connect  ->  Kafka 4.2 (KRaft), Avro + Schema Registry (BACKWARD)
  |  exactly-once into Flink state: offsets, guard state and pending commits share one checkpoint
  v
Flink 1.20 job (one job, every table)
  decode -> keyBy(pk) -> sequence guard -> Iceberg upsert sink (equality deletes)
          \-> append-only changelog table (every Kafka record, before the guard)
  |  exactly-once commit per checkpoint; committer skips checkpoints already in snapshot summaries
  v
Iceberg 1.12 tables, REST catalog backed by Postgres, data on S3 (MinIO)
  |
  +-- Trino 484 (queries, reconciliation, maintenance)
  +-- controller (schema sync, job supervision, connector restarts)
  +-- Prometheus + Grafana
```

## Consistency model

"Exactly-once" here means: after the source goes quiet, every Iceberg table equals its Postgres table row for row, with no duplicate keys, and no snapshot ever shows an older version of a row than an earlier snapshot did. It does not mean each change is delivered once. Debezium delivers at least once and that is fine, because the sink is idempotent and ordered.

How each hop holds up its end:

1. **Capture.** The replication slot keeps WAL until Connect flushes offsets, so nothing committed is skipped. After a crash Debezium resends events since the last flush. Kafka keys events by primary key, so one key's events stay in one partition, in order.
2. **Ordering guard.** Upsert-by-key converges for in-order duplicates but not for a stale event that arrives late (a manual replay, a reprocessed batch). The job keys by primary key and keeps the highest applied `source.sequence` = [last commit LSN, change LSN] per key. That pair follows commit order. The change LSN on its own does not: a transaction can write a row before a snapshot point and commit after it, and its change LSN then sorts before the snapshot row. Rows from the initial snapshot apply only to keys with no streaming history; rows from incremental snapshots compare by sequence like any other event. Deletes are kept in state for 7 days, which matches the topics' `min.compaction.lag.ms`.
3. **Kafka to Flink.** The Kafka source, the guard state and the sink's pending manifests are snapshotted in the same checkpoint. On failure the job resumes from that checkpoint. Kafka consumer offsets are committed only for lag monitoring and are never used to resume.
4. **Flink to Iceberg.** Files are committed in one Iceberg commit per checkpoint, after the checkpoint completes. Each snapshot records `flink.job-id` and `flink.max-committed-checkpoint-id`; on restore the committer replays manifests for checkpoints that are not in the table yet and skips the rest.

The guard only works if per-key order survives the job: the decoder is chained to the source (no rebalance before `keyBy`), topic partition counts never change, and primary keys are never `DEFERRABLE`.

Evidence collected on every run (`chaos/reconcile.py`):
- state: Trino rows equal Postgres rows, typed comparison, zero duplicate keys, no Debezium TOAST placeholders
- changelog: no Kafka offset appears twice; every non-tombstone record in Kafka is in the changelog; per key, `version` (bumped by a trigger) has no gaps
- guard: `suspect_dropped` (dropping an event newer than what was applied) is zero
- snapshots: across every `orders` snapshot in a run, no key's version goes down and no deleted key comes back
- quiescent extras: foreign keys hold, and the generator's ledger matches Postgres (secondary, it is a dual write)

## Schema evolution

The Postgres connector has no schema-change topic. DDL becomes visible only as a new Avro schema on the next row of that table. There are two gates.

Gate 1 is Schema Registry in BACKWARD mode. It rejects narrowing (bigint to int) and cross-family changes (text to int). Debezium then fails, nothing reaches Kafka, and the slot-lag alert fires. `errors.tolerance=none` is what makes this a stop rather than a silent skip.

Gate 2 is the controller, which reads the Schema Registry version history. The job accepts only writer schema ids that the controller has mapped to Iceberg field ids (table property `cdc.schema-map`). An unknown id fails the job with a non-recoverable error. The controller applies the change, then resubmits from the last checkpoint, so a checkpoint never mixes two schemas. A repeat failure on the same id halts.

| DDL in Postgres | Outcome |
|---|---|
| ADD COLUMN, nullable | Iceberg column added, job restarted, old rows NULL (same as Postgres) |
| ADD COLUMN ... DEFAULT x | added, then an incremental snapshot backfills existing rows (Postgres 11+ fast defaults emit no row events) |
| int to bigint, numeric(p,s) to numeric(p+n,s) | Iceberg type promoted, restart |
| bigint to int | rejected by Schema Registry, pipeline stops; if intended, relax compatibility for that subject (the sink upcasts) |
| RENAME COLUMN | halts: in pgoutput a rename is a drop plus an add. `make schema-resolve` renames the Iceberg column, which keeps the field id and history |
| DROP COLUMN | halts. Resolve with `drop` (metadata-only Iceberg drop, history stays in time travel) or `keep` (column stays, updated rows get NULL) |
| numeric scale change | halts: Iceberg cannot represent it |
| primary key type change, TRUNCATE | halts: rebuild the table |

## Failure modes

| Failure | Expected behaviour |
|---|---|
| TaskManager killed | job restarts from the last checkpoint; files from the failed attempt become orphans |
| TaskManager killed after a checkpoint completed, before its Iceberg commit | committer replays that checkpoint's manifests on restore, once |
| JobManager killed (no HA in compose) | controller waits out the heartbeat timeout, then resubmits from the newest retained checkpoint in S3 |
| Kafka broker down | Debezium and Flink retry; Connect tasks that fail are restarted by the controller |
| Connect SIGKILL | Debezium resends since the last offset flush; the guard drops the duplicates |
| S3 unavailable | checkpoints fail (tolerable up to 5), commits wait, job resumes when S3 returns |
| Old Kafka records re-produced | the guard drops every one |
| Undecodable key in a CDC topic | sent to the `cdc.dlq` topic with error headers; it cannot belong to a row |
| Undecodable value, TRUNCATE, unknown schema after sync | job halts; skipping could lose a real change |
| Connector down for long | slot retains WAL up to `max_slot_wal_keep_size` (4 GB), alert at 1 GB; past that the slot is lost and the table needs a rebuild |

## Maintenance

Flink commits small data and equality-delete files every checkpoint. Trino `optimize` compacts them. It pins rewritten files to the starting snapshot's sequence number, so equality deletes committed during the rewrite still apply. Expiry and orphan removal run only after a checkpoint has completed since the job's last restore. Snapshot retention must exceed the longest tolerated job outage, because the committer finds its last commit by walking snapshot history. Orphan retention must exceed checkpoint timeout times tolerated failures, or it deletes files that are written but not yet committed.

## Alternatives considered

- **Spark Structured Streaming instead of Flink.** Upserts mean a MERGE per micro-batch, which joins against the table every batch. Per-key ordering state is possible but awkward. Flink has keyed state and a sink that writes equality deletes at each checkpoint.
- **Hudi or Delta instead of Iceberg.** Hudi's record index makes upserts cheaper on write and has a precombine field for ordering. Iceberg won on engine neutrality (Trino, Flink, Spark, Snowflake read the same tables), a REST catalog standard, and field-id schema evolution, which is what makes renames safe. The cost is that readers pay for equality deletes until compaction runs.
- **Debezium Server, Flink CDC or the Iceberg Kafka Connect sink instead of Connect + Kafka + Flink.** Each removes a hop. Kafka decouples the WAL slot from sink downtime, keeps a replayable log, and lets other consumers subscribe. Flink provides the keyed state the ordering guard needs. For pure replication without a guard, the Iceberg Kafka Connect sink would be the simpler choice.
- **Copy-on-write instead of merge-on-read.** Copy-on-write rewrites whole files for every update, which is too much write amplification for 30 s commits of scattered order updates. Merge-on-read writes are cheap and push the cost to readers, so compaction is required, not optional.
- **Flink SQL instead of DataStream.** `debezium-avro-confluent` dedups in arrival order, cannot see `source.sequence`, needs REPLICA IDENTITY FULL to decode updates, and fixes the schema at submit time.
- **Iceberg's DynamicIcebergSink (1.10+).** It evolves schemas in flight, but it keeps one writer per (table, schema id) and commits a checkpoint's writers in one RowDelta. An upsert to the same key on both sides of a mid-checkpoint schema change could leave two live rows, because equality deletes only apply to lower sequence numbers. Restarting from a checkpoint never mixes schemas.

## Metrics that define done

- p95 commit-to-queryable latency under steady load, measured from Postgres commit timestamps to Iceberg snapshot commit times (no polling)
- zero discrepancies on every check above, after each chaos scenario
- Trino query latency and file counts against number of commits, with and without maintenance (time-compressed, labelled)

## Changes after design review

- Debezium dead-letter settings removed: they only apply to sink connectors, and `errors.tolerance=all` on a source silently drops events.
- No schema-change topic exists for Postgres, so schema sync reads Schema Registry history.
- Upsert by key alone does not survive a stale replay; added the sequence guard. The first version ordered by change LSN, which drops updates from transactions that straddle a snapshot point.
- `ADD COLUMN ... DEFAULT` needs a backfill; tombstones carry no deletes (op=d does).
- The REST catalog fixture keeps its state in SQLite inside the container; it now uses Postgres.
- MinIO no longer publishes images; pinned the last Bitnami build.
- "Query latency after 72 hours" cannot be measured on a laptop; the benchmark compresses time and says so.
