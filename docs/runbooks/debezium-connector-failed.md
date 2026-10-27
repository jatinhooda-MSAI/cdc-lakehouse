# Debezium connector failed

Alert: `ConnectorFailed`, usually followed by `ReplicationSlotLagHigh`. The controller already restarts failed tasks with backoff (10 s, doubling, capped at 5 min) and logs each attempt, so a connector that stays failed is failing for a reason a restart cannot fix.

```
curl -s localhost:8083/connectors/shop-cdc/status | python3 -m json.tool
make logs | grep connector
```

Read the first line of the task trace:

- `Schema being registered is incompatible`: someone ran DDL that Schema Registry rejects in BACKWARD mode (narrowing a type, text to int). Nothing reached Kafka. If the change is intended and the sink can still represent it, relax compatibility for that subject only and let the controller restart the task: `curl -X PUT -H 'Content-Type: application/json' localhost:8085/config/dbserver.public.<table>-value -d '{"compatibility": "NONE"}'`. If the sink cannot represent it, the job will halt next (see pipeline-halted.md).
- connection refused or authentication errors against Postgres: fix the database side; the task resumes from its stored offset.
- `replication slot ... does not exist` or `wal_status = lost`: the slot is gone, see replication-slot-filling-disk.md.
- broker errors: Kafka is down or full; check `docker compose ps kafka` and disk.

Do not switch the connector to `errors.tolerance=all` to get it moving. On a source connector that skips the failing events and commits offsets past them, so the lake silently diverges.
