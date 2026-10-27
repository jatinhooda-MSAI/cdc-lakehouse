# Checkpoint timeouts

Alert: `CheckpointsFailing`. Iceberg only commits when a checkpoint completes, so failing checkpoints mean fresh data stops showing up (`FreshnessHigh` follows within minutes) even though records keep flowing.

Look at the Flink UI (`localhost:8081`, Checkpoints tab) for which subtask did not acknowledge, and at the Grafana backpressure and checkpoint-size panels.

- Every subtask late, S3 errors in the TaskManager log: the object store is slow or down. Checkpoints and Iceberg file writes both go to it. In the chaos run a 150 s MinIO pause failed checkpoints and the job recovered without help once MinIO came back. Up to 5 consecutive failures are tolerated before the job restarts from the last good checkpoint.
- One operator backpressured: usually a writer stuck behind a slow commit or a burst (a 100k-row transaction lands in one checkpoint). Wait it out, or raise TaskManager memory or parallelism with a savepoint restart. Never change topic partition counts to "scale"; per-key order depends on them.
- Checkpoint size growing steadily: the guard keeps one small entry per key for 7 days. That is expected; RocksDB incremental checkpoints keep the per-checkpoint cost flat.

While checkpoints fail, do not run `remove_orphan_files` with a short retention. Files written for the stuck checkpoint are not committed yet, and orphan cleanup cannot tell them apart from garbage. The maintenance service only expires snapshots and removes orphans after a checkpoint has completed since the job's last restore.
