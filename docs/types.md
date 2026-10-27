# Type mapping

Every row of `type_samples` goes through this path and is compared value by value against Postgres on each reconcile.

| Postgres | Debezium Avro (`connect.name`) | Iceberg | Trino | What changes |
|---|---|---|---|---|
| smallint | int (int16) | int | integer | stored wider |
| integer | int | int | integer | |
| bigint | long | long | bigint | |
| numeric(p,s) | bytes, logical decimal(p,s) | decimal(p,s) | decimal(p,s) | exact; `decimal.handling.mode=precise` |
| real | float | float | real | |
| double precision | double | double | double | |
| boolean | boolean | boolean | boolean | |
| text, varchar(n), char(n) | string | string | varchar | the length limit is gone; char keeps its padding |
| date | int, `io.debezium.time.Date` | date | date | |
| time | long, `io.debezium.time.MicroTime` | time | time(6) | Flink's TIME holds milliseconds, so microseconds are truncated |
| timestamp | long, `io.debezium.time.MicroTimestamp` | timestamp | timestamp(6) | |
| timestamptz | string, `io.debezium.time.ZonedTimestamp` | timestamptz | timestamp(6) with time zone | normalized to UTC; the original offset is not kept |
| uuid | string, `io.debezium.data.Uuid` | uuid | uuid | |
| jsonb | string, `io.debezium.data.Json` | string | varchar | text as Postgres renders it; compare parsed, not as strings |
| bytea | bytes | binary | varbinary | `binary.handling.mode=bytes` |

Why not the other decimal modes: `double` turns 19.99 into 19.989999999999998, and `string` drops precision and scale from the schema, so the sink cannot pick a column type. Unconstrained `numeric` arrives as Debezium's variable-scale struct and has no Iceberg equivalent. The decoder treats it as schema drift and halts, so keep money columns as `numeric(p,s)`.

Not covered: arrays, enums, intervals, geometry, hstore, money. Each would need a mapping in `RowConverter` and `controller/schema.py` (they must agree) plus a column in `type_samples`.

Every table also gets three metadata columns: `_lsn` (WAL position of the change), `_op` (`c`, `u` or `r`; deletes remove the row) and `_source_ts` (commit time in Postgres, which the freshness metric reads).
