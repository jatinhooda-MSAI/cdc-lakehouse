# Pipeline halted

Alert: `PipelineHalted`. The controller cancelled the Flink job and will not resubmit it until someone decides. Nothing is lost while it waits: Debezium keeps writing to Kafka and the job resumes from its last checkpoint.

Find the reason:

```
make logs | grep HALT
docker compose run --rm tools python -c "from common import catalog; print({t[-1]: catalog().load_table(t).properties.get('cdc.halted') for t in catalog().list_tables('cdc')})"
```

| reason | what happened | resolution |
|---|---|---|
| `vN removed [col]; added [new], possibly a rename` | a column vanished from the source schema, usually `RENAME COLUMN` | `make schema-resolve TABLE=t ACTION=rename ARGS="old new"` keeps the Iceberg field id, so history stays under the new name |
| `vN removed [col]` | `DROP COLUMN` | `ACTION=drop ARGS=col` (metadata-only Iceberg drop, old values still reachable by time travel) or `ACTION=keep ARGS=col` (column stays; rows updated from now on get NULL) |
| `column c: decimal(12, 2) -> decimal(12, 4) is not representable` | a type change Iceberg cannot promote | rebuild the table (below) |
| `key schema changed` | the primary key type changed; Kafka partitioning and guard keys would split | rebuild the table |
| `unsupported-op ... op=t` | TRUNCATE reached the job (it is revoked for the app role, so someone used a superuser) | rebuild the table |
| `undecodable-value` | a record in a CDC topic has a valid key but a value that does not decode | inspect the offset in the reason; the job never skips it, because it may be a real change for that key |
| `unresolved schema-drift` | the same unknown writer schema failed twice after a sync | check Schema Registry versions against `cdc.schema-map` |

Rebuild a table: stop the job with a savepoint (`make upgrade` without code changes does this), drop the Iceberg table, let the controller recreate it from the latest schema, start the job with `.state/allow-non-restored` present, then signal an incremental snapshot for that table (`INSERT INTO debezium_signal ...`, see `controller/schema.py`). Rows deleted in Postgres before the snapshot will not come back, because the new table starts empty.
