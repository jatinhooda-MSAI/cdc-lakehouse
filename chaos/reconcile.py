import argparse
import json
import re
import sqlite3
import struct
import time
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from datetime import time as dtime
from decimal import Decimal

import requests
from confluent_kafka import Consumer, TopicPartition

from common import KAFKA, TABLES, TOPIC_PREFIX, log, pg, query
from generator.generator import LEDGER, encode

L = log("reconcile")
PLACEHOLDER = "__debezium_unavailable_value"
FLOAT4, JSONB = 700, 3802


def norm(v, oid):
    if v is None:
        return None
    if oid == FLOAT4:
        return struct.unpack("f", struct.pack("f", float(v)))[0]
    if oid == JSONB:
        return json.dumps(json.loads(v) if isinstance(v, str) else v, sort_keys=True)
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat() if v.tzinfo else v.isoformat()
    if isinstance(v, (date, dtime)):
        return v.isoformat()
    if isinstance(v, (bytes, memoryview, bytearray)):
        return bytes(v).hex()
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, Decimal):
        return str(v)
    return v


def pg_table(table):
    with pg(admin=True) as conn:
        cur = conn.execute(f"SELECT * FROM {table}")
        cols = [(d.name, d.type_code) for d in cur.description]
        return cols, cur.fetchall()


def state(table):
    cols, rows = pg_table(table)
    names = ", ".join(f'"{c}"' for c, _ in cols)
    ice = query(f"SELECT {names} FROM {table}")
    row = lambda r: tuple(norm(v, oid) for v, (_, oid) in zip(r, cols))
    want = {r[0]: row(r) for r in rows}
    counts = Counter(r[0] for r in ice)
    got = {r[0]: row(r) for r in ice}
    mismatched = [k for k in want.keys() & got.keys() if want[k] != got[k]]
    out = {
        "table": table, "postgres": len(want), "iceberg": len(ice),
        "missing": len(want.keys() - got.keys()), "extra": len(got.keys() - want.keys()),
        "mismatched": len(mismatched), "duplicates": sum(1 for n in counts.values() if n > 1),
        "placeholders": sum(1 for r in ice for v in r if v == PLACEHOLDER),
    }
    out["ok"] = not any(out[k] for k in ("missing", "extra", "mismatched", "duplicates", "placeholders"))
    if mismatched:
        k = sorted(mismatched)[0]
        out["sample"] = {c: [a, b] for (c, _), a, b in zip(cols, want[k], got[k]) if a != b}
    return out


def states(tables=TABLES):
    return [state(t) for t in tables]


def changelog():
    dup_offsets = query("SELECT count(*) FROM (SELECT kafka_topic, kafka_partition, kafka_offset FROM changelog "
                        "GROUP BY 1, 2, 3 HAVING count(*) > 1)")[0][0]
    redelivered = query("SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM changelog "
                        "GROUP BY table_name, pk, commit_lsn, lsn, op HAVING count(*) > 1)")[0][0]
    gaps = query("""
        WITH e AS (SELECT DISTINCT table_name, pk, commit_lsn, lsn, op, version FROM changelog
                   WHERE op IN ('c', 'u', 'r') AND version IS NOT NULL AND table_name IN ('customers', 'products', 'orders', 'order_items', 'payments', 'type_samples')),
             s AS (SELECT op, version,
                          lag(version) OVER (PARTITION BY table_name, pk ORDER BY commit_lsn, lsn) AS prev,
                          row_number() OVER (PARTITION BY table_name, pk ORDER BY commit_lsn, lsn) AS rn
                   FROM e)
        SELECT count(*) FROM s
        WHERE (rn = 1 AND op = 'c' AND version <> 1)
           OR (rn > 1 AND op IN ('c', 'u') AND version <> prev + 1)
           OR (rn > 1 AND op = 'r' AND version < prev)""")[0][0]
    rows = query("SELECT count(*) FROM changelog")[0][0]
    return {"rows": rows, "duplicate_offsets": dup_offsets, "redelivered_events": int(redelivered), "version_gaps": gaps,
            "ok": dup_offsets == 0 and gaps == 0}


def kafka_offsets(topics):
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"reconcile-{uuid.uuid4()}", "enable.auto.commit": False})
    present, tombstones = defaultdict(set), 0
    try:
        meta = c.list_topics(timeout=10).topics
        parts = [TopicPartition(t, p, 0) for t in topics for p in meta[t].partitions]
        ends = {(tp.topic, tp.partition): c.get_watermark_offsets(tp, timeout=10)[1] for tp in parts}
        c.assign(parts)
        remaining = {k for k, end in ends.items() if end > 0}
        while remaining:
            for m in c.consume(1000, timeout=5):
                if m.error():
                    continue
                k = (m.topic(), m.partition())
                if m.value() is None:
                    tombstones += 1
                else:
                    present[k].add(m.offset())
                if m.offset() + 1 >= ends[k]:
                    remaining.discard(k)
    finally:
        c.close()
    return present, tombstones


def completeness():
    topics = [f"{TOPIC_PREFIX}{t}" for t in TABLES]
    present, tombstones = kafka_offsets(topics)
    seen = defaultdict(set)
    for topic, part, off in query("SELECT kafka_topic, kafka_partition, kafka_offset FROM changelog WHERE kafka_topic IN (%s)"
                                  % ", ".join(f"'{t}'" for t in topics)):
        seen[(topic, part)].add(off)
    dlq = dlq_offsets()
    lost = sum(len(offs - seen[k] - dlq.get(k, set())) for k, offs in present.items())
    return {"kafka_records": sum(len(v) for v in present.values()), "tombstones": tombstones, "dlq": sum(len(v) for v in dlq.values()),
            "not_in_changelog": lost, "ok": lost == 0}


def dlq_offsets():
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"reconcile-dlq-{uuid.uuid4()}", "enable.auto.commit": False,
                  "auto.offset.reset": "earliest"})
    out = defaultdict(set)
    try:
        lo, hi = c.get_watermark_offsets(TopicPartition("cdc.dlq", 0), timeout=10)
        if hi > lo:
            c.assign([TopicPartition("cdc.dlq", 0, lo)])
            for _ in range(hi - lo):
                m = c.poll(5)
                if m is None:
                    break
                h = dict(m.headers() or [])
                out[(h["source.topic"].decode(), int(h["source.partition"]))].add(int(h["source.offset"]))
    finally:
        c.close()
    return out


def monotonic(since_ms, table="orders"):
    snaps = query(f'SELECT snapshot_id FROM "{table}$snapshots" WHERE committed_at >= from_unixtime_nanos({since_ms} * 1000000) '
                  "ORDER BY committed_at")
    prev, gone = {}, set()
    regressions = resurrected = 0
    for (sid,) in snaps:
        cur = dict(query(f"SELECT id, version FROM {table} FOR VERSION AS OF {sid}"))
        regressions += sum(1 for k, v in cur.items() if k in prev and v < prev[k])
        resurrected += sum(1 for k in cur if k in gone)
        gone = (gone | (prev.keys() - cur.keys())) - cur.keys()
        prev = cur
    return {"table": table, "snapshots": len(snaps), "regressions": regressions, "resurrected": resurrected,
            "ok": regressions == 0 and resurrected == 0}


def foreign_keys():
    checks = {
        "order_items->orders": "SELECT count(*) FROM order_items i LEFT JOIN orders o ON i.order_id = o.id WHERE o.id IS NULL",
        "payments->orders": "SELECT count(*) FROM payments p LEFT JOIN orders o ON p.order_id = o.id WHERE o.id IS NULL",
        "orders->customers": "SELECT count(*) FROM orders o LEFT JOIN customers c ON o.customer_id = c.id WHERE c.id IS NULL",
    }
    out = {k: query(v)[0][0] for k, v in checks.items()}
    out["ok"] = not any(out.values())
    return out


def guard_counters():
    text = requests.get("http://taskmanager:9249/metrics", timeout=10).text
    totals = defaultdict(float)
    for line in text.splitlines():
        m = re.match(r'flink_taskmanager_job_task_operator_table_(stale_dropped|suspect_dropped|dlq|tombstones)\{.*table="([^"]+)".*\} (\S+)', line)
        if m:
            totals[m.group(1)] += float(m.group(3))
    return {k: int(v) for k, v in totals.items()}


def ledger():
    db = sqlite3.connect(LEDGER)
    expected = defaultdict(dict)
    for tbl, pk, op, row in db.execute("SELECT tbl, pk, op, row FROM ops ORDER BY seq"):
        if op == "d":
            expected[tbl].pop(pk, None)
        else:
            expected[tbl][pk] = row
    out = {}
    with pg(admin=True) as conn:
        for tbl, rows in expected.items():
            cur = conn.execute(f"SELECT * FROM {tbl}")
            cols = [d.name for d in cur.description]
            actual = {r[0]: json.dumps(dict(zip(cols, r)), default=encode, sort_keys=True) for r in cur.fetchall()}
            out[tbl] = sum(1 for k in rows.keys() | actual.keys() if rows.get(k) != actual.get(k))
    return {"diffs": out, "ok": not any(out.values())}


def converge(timeout, tables=TABLES):
    start = time.time()
    while True:
        result = states(tables)
        if all(r["ok"] for r in result) or time.time() - start > timeout:
            return result, round(time.time() - start, 1)
        L.info("waiting: %s", ", ".join(f"{r['table']} {r['iceberg']}/{r['postgres']}" for r in result if not r["ok"]))
        time.sleep(10)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wait", type=float, default=0)
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--since-ms", type=int, default=0)
    ap.add_argument("--out")
    args = ap.parse_args()
    result, waited = converge(args.wait)
    report = {"state": result, "converged_after_s": waited}
    if args.full:
        report.update(changelog=changelog(), completeness=completeness(), foreign_keys=foreign_keys(),
                      guard=guard_counters(), ledger=ledger())
        if args.since_ms:
            report["monotonic"] = monotonic(args.since_ms)
    for r in result:
        print(f"{r['table']:<13} postgres={r['postgres']:<7} iceberg={r['iceberg']:<7} missing={r['missing']} extra={r['extra']} "
              f"mismatched={r['mismatched']} duplicates={r['duplicates']} placeholders={r['placeholders']}" + (f" sample={r['sample']}" if "sample" in r else ""))
    for k in ("changelog", "completeness", "foreign_keys", "guard", "ledger", "monotonic"):
        if k in report:
            print(f"{k}: {json.dumps(report[k], default=str)}")
    ok = all(r["ok"] for r in result) and all(report[k].get("ok", True) for k in ("changelog", "completeness", "foreign_keys", "ledger", "monotonic") if k in report)
    if "guard" in report:
        ok = ok and report["guard"].get("suspect_dropped", 0) == 0
    print("RECONCILE", "OK" if ok else "FAILED")
    if args.out:
        json.dump(report | {"ok": ok}, open(args.out, "w"), indent=2, default=str)
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
