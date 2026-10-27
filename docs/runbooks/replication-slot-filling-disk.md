# Replication slot filling disk

Alert: `ReplicationSlotLagHigh` (more than 1 GB of WAL held). Postgres keeps WAL for the `debezium` slot until Connect confirms it, so any outage between Postgres and Kafka turns into WAL on the database disk. `max_slot_wal_keep_size` is 4 GB here; past that Postgres invalidates the slot to protect itself.

```
docker compose exec postgres psql -U postgres -d shop -c "
  SELECT slot_name, active, wal_status,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS held
  FROM pg_replication_slots"
```

- `active = f`: the connector is not running. Fix that first (debezium-connector-failed.md); the lag drains once it reconnects.
- `active = t` but lag keeps growing: Connect is reading but not flushing offsets, usually because Kafka or Schema Registry is unavailable.
- Lag grows only while the captured tables are idle: the heartbeat should prevent this (`heartbeat.action.query` writes to a captured table every 10 s). Check that `debezium_heartbeat` is still in the publication.

If the slot will hit the limit before the cause is fixed, choose deliberately. You can raise `max_slot_wal_keep_size` if the disk allows. Or you can let the slot go and plan a rebuild. A lost slot cannot be resumed. A new snapshot only emits rows that still exist, so rows deleted during the gap would stay in Iceberg forever. Rebuild into fresh tables and swap them in instead of re-snapshotting over the old ones.

Never drop the slot to "clean up" while the connector is down. It is the only record of where replication stopped.
