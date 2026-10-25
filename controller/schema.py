import json

import requests
from prometheus_client import Counter, Gauge
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

from common import NAMESPACE, SR, TOPIC_PREFIX, catalog, log, pg

L = log("schema")
SKIP = {"debezium_signal"}
META = [("_lsn", LongType()), ("_op", StringType()), ("_source_ts", TimestamptzType())]
PROPS = {
    "format-version": "2",
    "write.upsert.enabled": "true",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
    "commit.retry.num-retries": "10",
}
HALTED = Gauge("cdc_controller_halted", "1 while a table waits for an operator decision", ["table"])
CHANGES = Counter("cdc_controller_schema_changes_total", "Schema changes applied to Iceberg", ["table", "kind"])
BACKFILLS = Counter("cdc_controller_backfills_total", "Incremental snapshots requested", ["table"])

SIMPLE = {"int": IntegerType(), "long": LongType(), "float": FloatType(), "double": DoubleType(),
          "boolean": BooleanType(), "string": StringType(), "bytes": BinaryType()}
NAMED = {
    "io.debezium.time.ZonedTimestamp": TimestamptzType(),
    "io.debezium.time.MicroTimestamp": TimestampType(),
    "io.debezium.time.Timestamp": TimestampType(),
    "io.debezium.time.NanoTimestamp": TimestampType(),
    "io.debezium.time.Date": DateType(),
    "io.debezium.time.MicroTime": TimeType(),
    "io.debezium.time.Time": TimeType(),
    "io.debezium.data.Uuid": UUIDType(),
}


def subject(table, kind="value"):
    return f"{TOPIC_PREFIX}{table}-{kind}"


def source_tables():
    subjects = requests.get(f"{SR}/subjects", timeout=10).json()
    names = [s[len(TOPIC_PREFIX):-len("-value")] for s in subjects if s.startswith(TOPIC_PREFIX) and s.endswith("-value")]
    return sorted(n for n in names if n not in SKIP)


def versions(table, kind="value"):
    return requests.get(f"{SR}/subjects/{subject(table, kind)}/versions", timeout=10).json()


def version_info(table, version="latest"):
    d = requests.get(f"{SR}/subjects/{subject(table)}/versions/{version}", timeout=10).json()
    env = json.loads(d["schema"])
    for f in env["fields"]:
        if f["name"] in ("before", "after"):
            for t in f["type"]:
                if isinstance(t, dict) and t.get("type") == "record":
                    return d["id"], {x["name"]: x["type"] for x in t["fields"]}
    raise ValueError(f"no value record in {subject(table)} v{version}")


def key_fields(table):
    s = json.loads(requests.get(f"{SR}/subjects/{subject(table, 'key')}/versions/latest", timeout=10).json()["schema"])
    return [f["name"] for f in s["fields"]]


def mapping(fields, schema):
    ids = {f.name: f.field_id for f in schema.fields}
    return {f: ids[f] for f in fields if f in ids}


def ice_type(t):
    if isinstance(t, list):
        t = next(x for x in t if x != "null")
    if isinstance(t, str):
        return SIMPLE[t]
    if isinstance(t.get("type"), (dict, list)):
        return ice_type(t["type"])
    if t.get("logicalType") == "decimal":
        return DecimalType(int(t["precision"]), int(t["scale"]))
    return NAMED.get(t.get("connect.name")) or SIMPLE[t["type"]]


def writable(src, dst):
    if src == dst:
        return True
    if isinstance(dst, LongType) and isinstance(src, IntegerType):
        return True
    if isinstance(dst, DoubleType) and isinstance(src, FloatType):
        return True
    if isinstance(dst, DecimalType) and isinstance(src, DecimalType):
        return src.scale <= dst.scale and src.precision - src.scale <= dst.precision - dst.scale
    return False


def promotable(cur, new):
    if isinstance(cur, IntegerType) and isinstance(new, LongType):
        return True
    if isinstance(cur, FloatType) and isinstance(new, DoubleType):
        return True
    if isinstance(cur, DecimalType) and isinstance(new, DecimalType):
        return new.scale == cur.scale and new.precision > cur.precision
    return False


def prop_set(table, key):
    return {x for x in table.properties.get(key, "").split(",") if x}


def ensure_namespace(cat):
    try:
        cat.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass


def ensure_changelog(cat):
    try:
        cat.load_table((NAMESPACE, "changelog"))
    except NoSuchTableError:
        cols = [("table_name", StringType(), True), ("pk", StringType(), True), ("op", StringType(), True),
                ("commit_lsn", LongType(), False), ("lsn", LongType(), False), ("version", LongType(), False),
                ("source_ts", TimestamptzType(), False), ("kafka_topic", StringType(), True),
                ("kafka_partition", IntegerType(), True), ("kafka_offset", LongType(), True), ("kafka_ts", TimestamptzType(), False)]
        schema = Schema(*[NestedField(i + 1, n, t, required=r) for i, (n, t, r) in enumerate(cols)])
        cat.create_table((NAMESPACE, "changelog"), schema, properties=PROPS | {"cdc.changelog": "true", "write.upsert.enabled": "false"})
        L.info("created changelog table")


def create(cat, table):
    schema_id, fields = version_info(table)
    pk = key_fields(table)
    cols = [(n, ice_type(t), n in pk) for n, t in fields.items()] + [(n, t, False) for n, t in META]
    schema = Schema(*[NestedField(i + 1, n, t, required=r) for i, (n, t, r) in enumerate(cols)],
                    identifier_field_ids=[i + 1 for i, (n, _, _) in enumerate(cols) if n in pk])
    tbl = cat.create_table((NAMESPACE, table), schema, properties=PROPS | {"cdc.topic": f"{TOPIC_PREFIX}{table}"})
    smap = {}
    for v in versions(table):
        vid, vfields = version_info(table, v)
        smap[str(vid)] = mapping(vfields, tbl.schema())
    with tbl.transaction() as tx:
        tx.set_properties({"cdc.sr-version": str(versions(table)[-1]), "cdc.schema-map": json.dumps(smap, sort_keys=True)})
    CHANGES.labels(table, "create").inc()
    L.info("created %s with %d columns, pk=%s", table, len(cols), pk)


def pg_default(table, column):
    with pg(admin=True) as conn:
        row = conn.execute(
            "SELECT column_default FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s AND column_name = %s",
            (table, column),
        ).fetchone()
    return row[0] if row and row[0] and not row[0].startswith("nextval(") else None


def request_backfill(table):
    with pg(admin=True) as conn:
        conn.execute(
            "INSERT INTO debezium_signal (id, type, data) VALUES (gen_random_uuid()::text, 'execute-snapshot', %s)",
            (json.dumps({"data-collections": [f"public.{table}"], "type": "incremental"}),),
        )
    BACKFILLS.labels(table).inc()
    L.info("requested incremental snapshot of %s", table)


def halt(tbl, table, reason):
    if tbl.properties.get("cdc.halted") != reason:
        with tbl.transaction() as tx:
            tx.set_properties({"cdc.halted": reason})
        L.error("HALT %s: %s", table, reason)
    HALTED.labels(table).set(1)
    return reason


def evolve(cat, table):
    tbl = cat.load_table((NAMESPACE, table))
    if tbl.properties.get("cdc.halted"):
        HALTED.labels(table).set(1)
        return tbl.properties["cdc.halted"]
    HALTED.labels(table).set(0)
    if len(versions(table, "key")) > 1:
        return halt(tbl, table, "key schema changed; partitioning and guard keys would split, rebuild required")
    synced = int(tbl.properties.get("cdc.sr-version", "0"))
    all_versions = versions(table)
    for v in [x for x in all_versions if x > synced]:
        older = [x for x in all_versions if x < v]
        prev = version_info(table, older[-1])[1] if older else {}
        schema_id, cur = version_info(table, v)
        accepted = str(v) in prop_set(tbl, "cdc.resolved-versions")
        removed = sorted(f for f in prev if f not in cur)
        if removed and not accepted:
            added = sorted(f for f in cur if f not in prev)
            hint = f"; added {added}, possibly a rename" if added else ""
            return halt(tbl, table, f"v{v} removed {removed}{hint}; run make schema-resolve")
        names = {f.name: f for f in tbl.schema().fields}
        adds, widens, backfill = [], [], False
        for f, t in cur.items():
            new = ice_type(t)
            if f in names:
                current = names[f].field_type
                if writable(new, current):
                    continue
                if promotable(current, new):
                    widens.append((f, new))
                    continue
                return halt(tbl, table, f"v{v} column {f}: {current} -> {new} is not representable in Iceberg")
            adds.append((f, new))
            backfill = backfill or pg_default(table, f) is not None
        if adds or widens:
            with tbl.update_schema() as u:
                for f, t in adds:
                    u.add_column(f, t, required=False)
                for f, t in widens:
                    u.update_column(f, field_type=t)
            tbl = cat.load_table((NAMESPACE, table))
        smap = json.loads(tbl.properties.get("cdc.schema-map", "{}"))
        smap[str(schema_id)] = mapping(cur, tbl.schema())
        with tbl.transaction() as tx:
            tx.set_properties({"cdc.sr-version": str(v), "cdc.schema-map": json.dumps(smap, sort_keys=True)})
        tbl = cat.load_table((NAMESPACE, table))
        for f, t in adds:
            CHANGES.labels(table, "add").inc()
            L.info("%s v%d: added column %s %s", table, v, f, t)
        for f, t in widens:
            CHANGES.labels(table, "widen").inc()
            L.info("%s v%d: widened %s to %s", table, v, f, t)
        if removed:
            CHANGES.labels(table, "resolved-removal").inc()
            L.info("%s v%d: accepted removal of %s", table, v, removed)
        if backfill:
            request_backfill(table)
    return None


def sync():
    cat = catalog()
    ensure_namespace(cat)
    ensure_changelog(cat)
    existing = {t[-1] for t in cat.list_tables(NAMESPACE)}
    halted, created = {}, []
    for table in source_tables():
        if table not in existing:
            create(cat, table)
            created.append(table)
            continue
        reason = evolve(cat, table)
        if reason:
            halted[table] = reason
    return halted, created
