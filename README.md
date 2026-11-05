# cdc-lakehouse

Postgres replicated into Apache Iceberg through Debezium, Kafka and Flink, queried with Trino, all in docker-compose. One Flink job writes every table as upserts with equality deletes. A per-key ordering guard keeps replayed or re-sent events from rolling a row back. The chaos suite kills each component under load and then compares every Iceberg table with Postgres row by row.

Results:
- 12 injected faults across 9 scenarios ended with 0 differing rows. No order's version went backwards in any `orders` snapshot taken during the runs.
- Running the same replay with the guard switched off left 975 wrong rows.
- With 30 s checkpoints, p95 from Postgres commit to queryable in Trino is 30.4 s.

```
Postgres 16 ──pgoutput──> Debezium 3.7 (Kafka Connect) ──Avro──> Kafka 4.2 + Schema Registry
                at least once, per-key order                           │
                                                                       │ offsets, guard state and pending
                                                                       │ commits in one checkpoint
                                                                       v
Trino 484 <── Iceberg 1.12 on S3, REST catalog <──one commit per checkpoint── Flink 1.20 job
                                                                               decode > keyBy(pk) > guard > upsert
                                                                               every Kafka record > changelog table
controller: creates and evolves tables from Schema Registry, resubmits the job from its newest checkpoint, restarts failed connector tasks
```

## Running it

Needs Docker with about 8 GB for the VM. Everything else builds in containers.

```
make up          # build the Flink job and images, start the stack, wait until healthy
make seed        # bulk-load the shop, register Debezium (initial snapshot), reconcile
make chaos       # failure scenarios under load, each followed by a full reconcile
```

Other targets: `make load` (steady workload with latency probes), `make reconcile`, `make evolve` (schema change suite, ends with one table halted on purpose), `make bench-latency`, `make bench-maintenance`, `make maintain`, `make upgrade` (rebuild the job and restart it through a savepoint), `make report` (rewrites the tables below from `results/`), `make trino`, `make reset`.

UIs: Flink `localhost:8081`, Trino `localhost:18080`, Grafana `localhost:3000`, Prometheus `localhost:9090`, MinIO `localhost:9001`.

## Results

### Failure injection

`make reset && make up && make seed && make chaos` ran unattended in 57 minutes on an M-series MacBook with 7.6 GiB for the Docker VM. During every scenario the generator runs at 100 ops/s. The load covers inserts, order status updates, cascading deletes, and price updates on products whose 4.8 KB descriptions Postgres stores out of line (TOAST). It also includes primary key updates, a 300 to 600 row transaction every two minutes, and one latency probe per second. Every scenario ends with the drain and every check listed under "How correctness is checked". "Fault held" is how long the component was down or paused. "Fresh data after heal" is the time from the component coming back until a probe row written at that moment was queryable in Trino. Raw output is in `results/chaos/`.

<!-- chaos -->
| scenario | runs | fault held | evidence the fault landed | row diffs | version regressions | fresh data after heal | worst latency | result |
|---|---|---|---|---|---|---|---|---|
| tm-kill-during-checkpoint | 3 | 5 s | chk 3 failed, restored 2; chk 7 failed, restored 6; chk 11 failed, restored 10 | 0 | 0 | 47 s | 81 s | pass |
| tm-kill-before-iceberg-commit | 2 | 3 s | chk 21 committed 1x; chk 26 committed 1x | 0 | 0 | 55 s | 60 s | pass |
| jobmanager-kill | 1 | 10 s | resumed from chk-34 | 0 | 0 | 59 s | 79 s | pass |
| kafka-broker-kill | 1 | 60 s | 365 events re-sent after the outage | 0 | 0 | 50 s | 108 s | pass |
| connect-sigkill | 1 | 10 s | 3244 events re-sent | 0 | 0 | 40 s | 49 s | pass |
| minio-pause-150s | 1 | 150 s | no commits during pause, 1 checkpoint(s) failed | 0 | 0 | 9 s | 167 s | pass |
| replay-old-offsets | 1 | - | 2100 old records re-produced, 2100 dropped | 0 | 0 | - | 32 s | pass |
| poison-record | 1 | - | 1 record in DLQ | 0 | 0 | - | 31 s | pass |
| compaction-under-load | 1 | - | orders ok, order_items ok, payments ok | 0 | 0 | - | 32 s | pass |
| control-replay-without-guard | 1 | - | 2100 old records re-produced, 0 dropped | 975 | 907 | - | 32 s | fails as expected; incremental snapshot left 108 diffs, deleting 108 resurrected keys left 0 |
<!-- /chaos -->

### Latency

`make bench-latency`: 10 minutes at 100 ops/s for each checkpoint interval, with one probe row per second. Latency is the commit time of the first Iceberg snapshot that contains the probe, minus the probe's Postgres commit timestamp (`track_commit_timestamp`). Both clocks are inside the Docker VM, and there is no polling error, because each snapshot is read with time travel. Most of the time is spent waiting for the next checkpoint, so p95 should sit near the interval plus commit time, and it does.

<!-- latency -->
| checkpoint | probes | p50 | p95 | p99 | Postgres to Kafka p50 | Kafka to Iceberg p50 |
|---|---|---|---|---|---|---|
| 10 s | 598 | 6.6 s | 11.1 s | 11.8 s | 0.51 s | 6.1 s |
| 30 s | 599 | 17.1 s | 30.4 s | 31.7 s | 0.49 s | 16.5 s |
| 60 s | 599 | 32.2 s | 59.2 s | 61.5 s | 0.50 s | 31.9 s |
<!-- /latency -->

### Schema changes

`make evolve` runs DDL against the live pipeline, one statement at a time. Each step waits for the expected outcome, applies the documented resolution, and reconciles that table against Postgres.

<!-- evolution -->
| DDL | expected | observed | resolution | job restarts | rows match |
|---|---|---|---|---|---|

<!-- /evolution -->

### Compaction

`make bench-maintenance` compresses time. One job writes the same stream to `orders` and to `orders_raw`, and both start from the same compacted copy. Checkpoints run every 5 s under 200 ops/s, which gave 420 commits in an hour at 681 rows per commit. Every 120 commits, `orders` gets Trino `optimize`, `optimize_manifests` and `expire_snapshots`; `orders_raw` never does. At the default 30 s interval, 120 commits is one hour and 420 is three and a half; rows per commit would be about four times higher, but the number of files per commit is the same. Query times are medians of five runs, taken with the metrics exporter paused. Vertical lines mark maintenance runs.

Read cost follows the delete-file count, not the row count. Each commit added about 7 files: per writer, one data file plus equality and position deletes. That is roughly 21,000 files per table per day at 30 s checkpoints if nothing compacts them. Before the bounded heap (see the notes below), the unmaintained table killed Trino at about 170 commits.

<!-- maintenance -->
![query latency and delete files vs commits](results/maintenance.png)

| | start | never maintained, 420 commits | maintained, right after the run at 360 | maintained, 420 commits |
|---|---|---|---|---|
| GROUP BY status, ms | 602 | 9712 | 612 | 1928 |
| count(*), ms | 103 | 9255 | 174 | 2849 |
| point lookup, ms | 280 | 9465 | 499 | 2280 |
| data files | 1 | 1041 | 9 | 311 |
| delete files | 0 | 2080 | 16 | 620 |
<!-- /maintenance -->

### Footprint

Peak memory per container, sampled every minute during `make chaos`.

<!-- footprint -->
| container | peak during chaos | limit |
|---|---|---|
| taskmanager | 1537 MiB | 1800 MiB |
| trino | 1279 MiB | 1280 MiB |
| jobmanager | 740 MiB | 896 MiB |
| connect | 652 MiB | 768 MiB |
| kafka | 487 MiB | 512 MiB |
| schema-registry | 332 MiB | 448 MiB |
| iceberg-rest | 228 MiB | 320 MiB |
| prometheus | 203 MiB | 256 MiB |
| grafana | 174 MiB | 192 MiB |
| minio | 129 MiB | 256 MiB |
| postgres | 125 MiB | 320 MiB |
| controller | 103 MiB | 160 MiB |
| exporter | 40 MiB | 160 MiB |
| maintenance | 39 MiB | 128 MiB |
| kafka-exporter | 23 MiB | 64 MiB |
| postgres-exporter | 17 MiB | 64 MiB |
| total | 6.0 GiB | |
<!-- /footprint -->

## How correctness is checked

`chaos/reconcile.py` runs after every scenario, once the pipeline has drained. Draining means the slot has confirmed the WAL position taken after the generator stopped, Flink's committed offsets have reached the topic ends, and two more checkpoints have completed.

- State: every column of every row in Trino equals Postgres (typed compare), no duplicate keys, no Debezium TOAST placeholders.
- Changelog: the job also appends every Kafka record it reads to `cdc.changelog`. No `(topic, partition, offset)` appears twice there, and every non-tombstone record in Kafka appears in it. This proves the Kafka to Iceberg hop is exactly-once on its own, separately from the upsert tables.
- Versions: a trigger bumps `version` on every update. Per key, the versions seen in the changelog have no gaps, so capture lost nothing in between.
- Snapshots: across every `orders` snapshot taken during the scenario, no key's version goes down and no deleted key comes back.
- Guard: `suspect_dropped`, the number of times the guard dropped an event newer than the row it had applied, must be 0.
- Foreign keys hold in Iceberg once quiet, and the generator's SQLite ledger matches Postgres. The ledger is a secondary check, because it is a dual write.

## Why these choices

A DataStream job instead of Flink SQL. The SQL Debezium format dedups in arrival order, cannot see `source.sequence`, needs REPLICA IDENTITY FULL on every table to decode updates, and fixes the schema when the job is submitted. The job here is about 800 lines of Java. That is the cost of a guard that holds when an old offset is replayed (see the negative control row above).

Ordering by `source.sequence`, not by LSN. The first version of the guard compared change LSNs. In review that turned out to drop updates from any transaction that wrote a row before a snapshot point and committed after it. Debezium's `[last commit LSN, change LSN]` pair follows commit order; the change LSN alone does not.

Fail fast on schema change instead of Iceberg's DynamicIcebergSink. The dynamic sink evolves tables in flight, but it keeps one writer per schema and commits them together. An upsert that straddles a mid-checkpoint schema change could leave two live rows for one key. Restarting from the last checkpoint after the controller has altered the table costs one restart per DDL and never mixes schemas.

A controller instead of Flink HA. Compose has no Kubernetes HA, and ZooKeeper only for Flink did not seem worth it. The controller resumes from the newest retained checkpoint in S3, waits out the heartbeat timeout after a JobManager loss, and refuses to start from the earliest offsets into non-empty tables (compacted topics drop deletes). On Kubernetes, the Flink operator's last-state upgrades would replace that part.

Iceberg REST catalog on Postgres. The reference fixture stores its catalog in SQLite under `/tmp`, so recreating the container emptied the catalog while the data stayed in S3. The image here adds the Postgres JDBC driver.

## Notes from building it

- The design went through two review passes before any code. The first found that Kafka Connect's dead-letter queue only exists for sink connectors (`errors.tolerance=all` on Debezium silently skips events), that the Postgres connector has no schema-change topic, and that `ADD COLUMN ... DEFAULT` never emits row events. The second found the LSN bug in my first guard design.
- MinIO's official images are gone from Docker Hub and quay. The stack pins the last Bitnami legacy build.
- The first job submission failed with "Equality field columns shouldn't be empty". The append-only changelog table had inherited `write.upsert.enabled=true` from the shared table properties, and FlinkSink reads that property.
- Jobs submitted through Flink's REST API ran at parallelism 1, although `parallelism.default` was 2 in the cluster config. The job now sets it.
- In the first chaos run, "kill the TaskManager during a checkpoint" mostly landed after the checkpoint had already completed (they take under a second). The harness now kills at the predicted trigger time and records the checkpoint's final status, so the table shows whether the fault landed.
- The first chaos run also died on a unique-email violation. Generator emails were derived from the seed, so a second run with the same seed collided with the first.
- Trino's host port is 18080, because 8080 was taken on this laptop.
- The first compaction benchmarks kept losing Trino. It died with no Java error, three times, while scanning tables full of equality deletes. The image sizes the heap at 80% of the container, which left too little room for native memory inside a VM that was already near full. A fixed 896 MB heap, G1 and 4 worker threads fixed it; the rerun had no restarts. The benchmark also records a failed query as a data point instead of crashing.
- `make seed` failed in the first clean run. Its reconcile started before the controller had created the tables, and it treated "table not found" as fatal instead of "not yet".

## What I'd do differently

- Run Flink under the Kubernetes operator with Kubernetes HA and last-state upgrades, and keep only schema sync from the controller. I left Kubernetes out: kind, the operator and cert-manager do not fit next to this stack in an 8 GB Docker VM, and an untested `k8s/` folder proves nothing.
- Move to Postgres 17 failover slots (`sync_replication_slots`). Today a primary failover loses the slot, and losing the slot means a rebuild.
- Bucket large tables by key (`bucket(16, id)`) so equality deletes only touch their own partition. At this data size unpartitioned was better, because partitions multiply the small files written per checkpoint.
- Use a production catalog (Polaris, Glue or Lakekeeper) with vended credentials instead of the reference REST server with static S3 keys.
- Turn on Kafka Connect exactly-once source support (KIP-618) to cut redeliveries. The guard would stay, because replays and re-snapshots happen anyway.
- Expose a cross-table consistent read point if consumers need joins that never see an order without its items: Debezium transaction metadata plus a "last checkpoint committed to every table" marker.

## Design

[docs/design.md](docs/design.md) covers the consistency model, the schema evolution policy, failure modes and the alternatives that lost. [docs/types.md](docs/types.md) maps every Postgres type through Avro to Iceberg. Runbooks are in [docs/runbooks](docs/runbooks).

## Versions

Postgres 16.15, Debezium 3.7.0.Final on Confluent Kafka Connect 8.2.4, Kafka 4.2.2 (KRaft), Schema Registry 8.2.4, Flink 1.20.5 with flink-connector-kafka 3.4.0-1.20, Iceberg 1.12.0, Iceberg REST catalog fixture 1.10.1 on Postgres JDBC, MinIO 2025-07-23 (Bitnami legacy build), Trino 484, Prometheus 3.15.1, Grafana 12.4.12, Python 3.11.
