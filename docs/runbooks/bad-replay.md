# Rows rolled back or resurrected after a replay

Signal: reconcile reports `mismatched` or `extra` rows, or `monotonic` reports regressions or resurrected keys. That can only happen when stale events reached a table without the guard. Typical causes: a job run with `--guard false`, a sink written by another tool, or a restore from state older than the table.

Repair, in this order:

1. Fix stale values. Signal an incremental snapshot of the table: `INSERT INTO debezium_signal (id, type, data) VALUES (gen_random_uuid()::text, 'execute-snapshot', '{"data-collections": ["public.orders"], "type": "incremental"}')`. Debezium re-reads every live row and emits it in commit order with the streaming changes, and the guard applies it.
2. Remove resurrected rows. A snapshot only emits rows that still exist, so keys deleted in Postgres stay in Iceberg. Delete every key that is in Iceberg but not in Postgres (`delete_zombies` in `chaos/chaos.py` does this in batches through Trino). This is safe while Flink runs only because these keys are never written again. Primary keys here come from sequences and are not reused.
3. Run `make reconcile`.

The negative control in the chaos suite runs exactly this procedure, and its row in the results table shows what each step left behind.
