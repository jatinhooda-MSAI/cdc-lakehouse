# Table has 10k small files

Signal: `lakehouse_files` climbing on the Grafana files panel, `lakehouse_scan_seconds` rising with it.

Every checkpoint, every writer subtask writes a data file, an equality-delete file and usually a position-delete file for each table that changed. The compaction benchmark measured about 7 files per commit with two writers. At 30 s checkpoints that is roughly 21,000 files per table per day before compaction. Readers must apply every equality-delete file to every older data file, so reads slow down long before storage becomes a problem. In the benchmark, 420 unmaintained commits made queries on `orders` about 15 times slower, and with Trino's default heap sizing they killed the server.

```
make trino
SELECT content, count(*), sum(file_size_in_bytes) FROM "orders$files" GROUP BY 1;
SELECT count(*) FROM "orders$snapshots";
```

`content` 0 is data, 1 is position deletes, 2 is equality deletes.

Fix:

1. `make maintain` runs Trino `optimize`, `optimize_manifests`, `expire_snapshots` and `remove_orphan_files` on every CDC table. `optimize` is safe while Flink keeps committing. It rewrites files at the starting snapshot's sequence number, so equality deletes committed during the rewrite still apply to the new files.
2. Check `lakehouse_maintenance_runs_total{result="failed"}`. A failed optimize leaves the table as it was; the next cycle retries.
3. If compaction cannot keep up, commit less often (see the latency table in the README for what each interval costs) or bucket the table by its key (`bucket(16, id)`). Partition columns must be part of the equality key in upsert mode.

Retention rules the maintenance service follows:
- Snapshot retention must exceed the longest job outage you can tolerate. On restore the Flink committer walks snapshot history to find its last commit. If that snapshot was expired, it commits the restored files a second time.
- Orphan retention must exceed checkpoint timeout times tolerated failures, plus outage time.
