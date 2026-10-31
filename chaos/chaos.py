import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import docker
import psycopg
import requests
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka import ConsumerGroupTopicPartitions

from bench.latency import summarize
from chaos import reconcile
from common import CONNECT, CONNECTOR, FLINK, GROUP, JOB_NAME, KAFKA, TABLES, TOPIC_PREFIX, get, log, pg, query, wait_for

L = log("chaos")
DOCKER = docker.from_env()
RESULTS = "/app/results/chaos"
ARGS_FILE = "/app/flink/job.args"


def box(name):
    return DOCKER.containers.get(f"cdc-{name}-1")


def now_ms():
    return int(time.time() * 1000)


def job():
    jobs = [j for j in get(f"{FLINK}/jobs/overview")["jobs"] if j["name"] == JOB_NAME and j["state"] == "RUNNING"]
    return jobs[0] if jobs else None


def vertices_running(jid):
    return all(v["status"] == "RUNNING" for v in get(f"{FLINK}/jobs/{jid}")["vertices"])


def checkpoints(jid):
    return get(f"{FLINK}/jobs/{jid}/checkpoints")


def completed_id(jid):
    latest = checkpoints(jid).get("latest") or {}
    return (latest.get("completed") or {}).get("id", 0)


def connector_running():
    st = get(f"{CONNECT}/connectors/{CONNECTOR}/status")
    return st["connector"]["state"] == "RUNNING" and all(t["state"] == "RUNNING" for t in st["tasks"])


def slot_active():
    with pg(admin=True) as c:
        row = c.execute("SELECT active FROM pg_replication_slots WHERE slot_name = 'debezium'").fetchone()
    return bool(row and row[0])


def probe_max():
    with pg(admin=True) as c:
        return c.execute("SELECT coalesce(max(id), 0) FROM latency_probe").fetchone()[0]


def healthy():
    j = job()
    return j and vertices_running(j["jid"]) and connector_running() and slot_active()


def wait_recovered(t_heal, timeout=420):
    target = probe_max()
    wait_for(healthy, timeout, what="pipeline healthy")
    t_running = time.time()
    wait_for(lambda: query(f"SELECT count(*) FROM latency_probe WHERE id >= {target}")[0][0] > 0, timeout, 1, "probe visible")
    t_fresh = time.time()
    c0 = wait_for(lambda: job() and completed_id(job()["jid"]), timeout, what="running job")
    wait_for(lambda: job() and completed_id(job()["jid"]) >= c0 + 2, timeout, what="two checkpoints after recovery")
    return {"to_running_s": round(t_running - t_heal, 1), "to_caught_up_s": round(t_fresh - t_heal, 1)}


def tm_metric(name):
    text = requests.get("http://taskmanager:9249/metrics", timeout=10).text
    total = 0.0
    for line in text.splitlines():
        if line.startswith(f"flink_taskmanager_job_task_operator_table_{name}{{"):
            total += float(line.rsplit(" ", 1)[1])
    return int(total)


class Load:
    def __init__(self, rate=100, seed=31):
        self.p = subprocess.Popen([sys.executable, "-m", "generator.generator", "run", "--rate", str(rate), "--probe",
                                   "--big-txn-every", "120", "--seed", str(seed)])

    def stop(self):
        self.p.send_signal(signal.SIGTERM)
        self.p.wait(120)


def drain(timeout=600):
    with pg(admin=True) as c:
        lsn = c.execute("SELECT pg_current_wal_lsn()").fetchone()[0]
        wait_for(lambda: c.execute("SELECT confirmed_flush_lsn >= %s FROM pg_replication_slots WHERE slot_name = 'debezium'",
                                   (lsn,)).fetchone()[0], timeout, what="slot flushed")
    topics = [f"{TOPIC_PREFIX}{t}" for t in TABLES]
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": f"drain-{uuid.uuid4()}"})
    meta = consumer.list_topics(timeout=10).topics
    ends = {(t, p): consumer.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1] for t in topics for p in meta[t].partitions}
    consumer.close()
    admin = AdminClient({"bootstrap.servers": KAFKA})

    def caught_up():
        res = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(GROUP)])[GROUP].result()
        done = {(tp.topic, tp.partition): tp.offset for tp in res.topic_partitions}
        return all(done.get(k, -1) >= end for k, end in ends.items() if end > 0)

    wait_for(caught_up, timeout, what="flink offsets at end")
    c0 = wait_for(lambda: job() and completed_id(job()["jid"]), timeout, what="running job")
    wait_for(lambda: job() and completed_id(job()["jid"]) >= c0 + 2, timeout, what="final checkpoints")


def scenario_tm_inflight():
    jid = job()["jid"]
    head = wait_for(lambda: checkpoints(jid)["history"][0] if checkpoints(jid)["history"] else None, 120, what="checkpoint history")
    interval = get(f"{FLINK}/jobs/{jid}/checkpoints/config")["interval"]
    nxt = head["trigger_timestamp"] + interval
    while nxt < now_ms() + 2000:
        nxt += interval
    time.sleep((nxt - now_ms() + 150) / 1000)
    target = head["id"] + (nxt - head["trigger_timestamp"]) // interval
    box("taskmanager").kill()
    t0 = time.time()
    time.sleep(5)
    box("taskmanager").start()
    rec = wait_recovered(t0)
    jid = wait_for(lambda: job() and job()["jid"], 120, what="running job")
    history = {h["id"]: h["status"] for h in checkpoints(jid)["history"]}
    restored = (checkpoints(jid).get("latest") or {}).get("restored") or {}
    return rec | {"fault_s": 5, "killed_during_checkpoint": target, "that_checkpoint": history.get(target), "restored_from": restored.get("id")}


def snapshots_for_checkpoint(cid):
    out = {}
    for t in TABLES:
        out[t] = query(f"SELECT count(*) FROM \"{t}$snapshots\" WHERE summary['flink.max-committed-checkpoint-id'] = '{cid}'")[0][0]
    return out


def scenario_tm_pending_commit():
    jid = job()["jid"]
    c = completed_id(jid)
    wait_for(lambda: completed_id(jid) > c, 120, 0.5, "fresh checkpoint")
    time.sleep(5)
    box("iceberg-rest").pause()
    try:
        c0 = completed_id(jid)
        cid = wait_for(lambda: completed_id(jid) > c0 and completed_id(jid), 180, 0.5, "checkpoint completed with catalog paused")
        time.sleep(2)
        box("taskmanager").kill()
        t0 = time.time()
    finally:
        box("iceberg-rest").unpause()
    time.sleep(3)
    box("taskmanager").start()
    rec = wait_recovered(t0)
    counts = snapshots_for_checkpoint(cid)
    return rec | {"fault_s": 3, "pending_checkpoint": cid, "snapshots_for_checkpoint": counts,
                  "exactly_once_commit": all(n <= 1 for n in counts.values()) and any(n == 1 for n in counts.values())}


def scenario_jm_kill():
    old = job()["jid"]
    box("jobmanager").kill()
    t0 = time.time()
    time.sleep(10)
    box("jobmanager").start()
    rec = wait_recovered(t0, timeout=600)
    new = wait_for(lambda: job() and job()["jid"], 120, what="running job")
    restored = (checkpoints(new).get("latest") or {}).get("restored") or {}
    return rec | {"fault_s": 10, "old_job": old, "new_job": new, "restored_from": restored.get("external_path")}


def scenario_kafka_kill():
    box("kafka").kill()
    t0 = time.time()
    try:
        time.sleep(60)
    finally:
        box("kafka").start()
    return wait_recovered(t0, timeout=600) | {"fault_s": 60}


def scenario_connect_kill():
    before = reconcile.changelog()["redelivered_events"]
    box("connect").kill()
    t0 = time.time()
    time.sleep(10)
    box("connect").start()
    rec = wait_recovered(t0, timeout=600)
    return rec | {"fault_s": 10, "redelivered_before": before}


def catalog_pointer(table="orders"):
    with psycopg.connect("host=postgres dbname=iceberg_catalog user=iceberg password=iceberg") as c:
        return c.execute("SELECT metadata_location FROM iceberg_tables WHERE table_namespace = 'cdc' AND table_name = %s",
                         (table,)).fetchone()[0]


def scenario_minio_pause():
    jid = job()["jid"]
    failed = checkpoints(jid)["counts"]["failed"]
    before = catalog_pointer()
    box("minio").pause()
    t0 = time.time()
    try:
        time.sleep(150)
        moved = catalog_pointer() != before
    finally:
        box("minio").unpause()
    rec = wait_recovered(t0, timeout=600)
    now = wait_for(lambda: job() and job()["jid"], 120, what="running job")
    return rec | {"fault_s": 150, "committed_during_pause": moved,
                  "failed_checkpoints": checkpoints(now)["counts"]["failed"] - (failed if now == jid else 0)}


def old_records(topic, per_partition=700):
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"replay-{uuid.uuid4()}", "enable.auto.commit": False})
    out = []
    try:
        for p in c.list_topics(topic, timeout=10).topics[topic].partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            start = lo + (hi - lo) // 2
            c.assign([TopicPartition(topic, p, start)])
            got = 0
            while got < per_partition:
                m = c.poll(5)
                if m is None:
                    break
                if m.value() is not None:
                    out.append((p, m.key(), m.value(), m.headers()))
                    got += 1
            c.unassign()
    finally:
        c.close()
    return out


def scenario_replay(topic=f"{TOPIC_PREFIX}orders"):
    stale = tm_metric("stale_dropped")
    records = old_records(topic)
    prod = Producer({"bootstrap.servers": KAFKA, "enable.idempotence": True})
    for p, k, v, h in records:
        prod.produce(topic, value=v, key=k, partition=p, headers=h)
    prod.flush(30)
    t0 = time.time()
    time.sleep(70)
    return {"replayed": len(records), "stale_dropped": tm_metric("stale_dropped") - stale, "to_running_s": 0,
            "to_caught_up_s": round(time.time() - t0, 1)}


def scenario_poison(topic=f"{TOPIC_PREFIX}orders"):
    dlq = reconcile.dlq_offsets()
    prod = Producer({"bootstrap.servers": KAFKA})
    prod.produce(topic, key=b"not-avro", value=b"\x00\x00\x00\x00\x01garbage", partition=0)
    prod.flush(30)
    wait_for(lambda: sum(map(len, reconcile.dlq_offsets().values())) > sum(map(len, dlq.values())), 120, what="record in DLQ")
    return {"dlq_records": sum(map(len, reconcile.dlq_offsets().values())), "to_running_s": 0, "to_caught_up_s": 0}


def scenario_compaction():
    t0 = time.time()
    runs = []
    for table in ("orders", "order_items", "payments"):
        start = time.time()
        try:
            query(f"ALTER TABLE {table} EXECUTE optimize")
            runs.append({"table": table, "ok": True, "seconds": round(time.time() - start, 1)})
        except Exception as e:
            runs.append({"table": table, "ok": False, "error": str(e)[:200]})
    return {"optimize": runs, "to_running_s": 0, "to_caught_up_s": round(time.time() - t0, 1)}


def set_guard(enabled):
    jid = job()["jid"]
    if enabled:
        if os.path.exists(ARGS_FILE):
            os.remove(ARGS_FILE)
    else:
        open(ARGS_FILE, "w").write("--guard false\n")
    r = requests.post(f"{FLINK}/jobs/{jid}/stop", json={"drain": False, "targetDirectory": "s3://flink/savepoints"}, timeout=30)
    r.raise_for_status()
    wait_for(lambda: job() and job()["jid"] != jid and vertices_running(job()["jid"]), 300, what="job resubmitted")


def repair(table="orders"):
    with pg(admin=True) as c:
        c.execute("INSERT INTO debezium_signal (id, type, data) VALUES (gen_random_uuid()::text, 'execute-snapshot', %s)",
                  (json.dumps({"data-collections": [f"public.{table}"], "type": "incremental"}),))


def delete_zombies(table="orders"):
    with pg(admin=True) as c:
        live = {r[0] for r in c.execute(f"SELECT id FROM {table}").fetchall()}
    zombies = sorted(r[0] for r in query(f"SELECT id FROM {table}") if r[0] not in live)
    for i in range(0, len(zombies), 500):
        query(f"DELETE FROM {table} WHERE id IN ({', '.join(map(str, zombies[i:i + 500]))})")
    return len(zombies)


SCENARIOS = {
    "tm-kill-during-checkpoint": (scenario_tm_inflight, 3),
    "tm-kill-before-iceberg-commit": (scenario_tm_pending_commit, 2),
    "jobmanager-kill": (scenario_jm_kill, 1),
    "kafka-broker-kill": (scenario_kafka_kill, 1),
    "connect-sigkill": (scenario_connect_kill, 1),
    "minio-pause-150s": (scenario_minio_pause, 1),
    "replay-old-offsets": (scenario_replay, 1),
    "poison-record": (scenario_poison, 1),
    "compaction-under-load": (scenario_compaction, 1),
}


def run(name, fn, repeat, rate, control=False):
    L.info("=== %s x%d", name, repeat)
    since = now_ms()
    load = Load(rate)
    time.sleep(20)
    t_start = now_ms()
    evidence = []
    try:
        for _ in range(repeat):
            evidence.append(fn())
            time.sleep(10)
        time.sleep(20)
    finally:
        load.stop()
    t_end = now_ms()
    drain()
    if control:
        state, waited = reconcile.states(), 0
    else:
        state, waited = reconcile.converge(300)
    report = {
        "scenario": name, "runs": repeat, "control": control, "evidence": evidence, "state": state, "converged_after_s": waited,
        "changelog": reconcile.changelog(), "completeness": reconcile.completeness(), "monotonic": reconcile.monotonic(t_start),
        "foreign_keys": reconcile.foreign_keys(), "guard": reconcile.guard_counters(), "ledger": reconcile.ledger(),
        "latency": summarize(since, t_end), "window": [t_start, t_end],
    }
    report["diffs"] = sum(r["missing"] + r["extra"] + r["mismatched"] + r["duplicates"] + r["placeholders"] for r in state)
    report["ok"] = (report["diffs"] == 0 and report["changelog"]["ok"] and report["completeness"]["ok"] and report["monotonic"]["ok"]
                    and report["foreign_keys"]["ok"] and report["guard"].get("suspect_dropped", 0) == 0)
    os.makedirs(RESULTS, exist_ok=True)
    json.dump(report, open(f"{RESULTS}/{name}.json", "w"), indent=2, default=str)
    L.info("=== %s %s diffs=%d regressions=%d", name, "OK" if report["ok"] else "FAILED", report["diffs"], report["monotonic"]["regressions"])
    return report


def negative_control(rate):
    set_guard(False)
    try:
        report = run("control-replay-without-guard", scenario_replay, 1, rate, control=True)
    finally:
        set_guard(True)
    repair()
    time.sleep(60)
    drain()
    state, waited = reconcile.converge(120)
    after_snapshot = sum(r["missing"] + r["extra"] + r["mismatched"] + r["duplicates"] for r in state)
    zombies = delete_zombies()
    state, more = reconcile.converge(300)
    report["repaired"] = {"diffs_after_snapshot": after_snapshot, "zombies_deleted": zombies,
                          "diffs": sum(r["missing"] + r["extra"] + r["mismatched"] + r["duplicates"] for r in state), "after_s": waited + more}
    json.dump(report, open(f"{RESULTS}/control-replay-without-guard.json", "w"), indent=2, default=str)
    return report


def sample_memory(stop, peaks):
    while not stop.is_set():
        for c in DOCKER.containers.list(filters={"label": "com.docker.compose.project=cdc"}):
            name = c.labels.get("com.docker.compose.service")
            if name == "tools":
                continue
            m = c.stats(stream=False).get("memory_stats", {})
            used = (m.get("usage", 0) - m.get("stats", {}).get("inactive_file", 0)) / 2**20
            p = peaks.setdefault(name, {"peak_mib": 0.0, "limit_mib": m.get("limit", 0) / 2**20})
            p["peak_mib"] = max(p["peak_mib"], used)
        stop.wait(60)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--rate", type=float, default=100)
    ap.add_argument("--repeat", type=int)
    ap.add_argument("--no-control", action="store_true")
    args = ap.parse_args()
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    reports = []
    peaks, stop = {}, threading.Event()
    threading.Thread(target=sample_memory, args=(stop, peaks), daemon=True).start()
    for name, (fn, repeat) in SCENARIOS.items():
        if args.only and name not in args.only:
            continue
        reports.append(run(name, fn, args.repeat or repeat, args.rate))
    if not args.no_control and (not args.only or "control-replay-without-guard" in args.only):
        reports.append(negative_control(args.rate))
    print(f"\n{'scenario':<32} {'runs':>4} {'diffs':>6} {'regress':>7} {'running_s':>9} {'caught_up_s':>11}  result")
    for r in reports:
        ev = r["evidence"]
        run_s = max(e.get("to_running_s", 0) for e in ev)
        up_s = max(e.get("to_caught_up_s", 0) for e in ev)
        verdict = ("expected failure" if not r["ok"] else "unexpected pass") if r["control"] else ("OK" if r["ok"] else "FAILED")
        print(f"{r['scenario']:<32} {r['runs']:>4} {r['diffs']:>6} {r['monotonic']['regressions']:>7} {run_s:>9} {up_s:>11}  {verdict}")
    stop.set()
    os.makedirs(RESULTS, exist_ok=True)
    json.dump(peaks, open(f"{RESULTS}/footprint.json", "w"), indent=2)
    ok = all(r["ok"] for r in reports if not r["control"]) and all(not r["ok"] and r.get("repaired", {}).get("diffs") == 0 for r in reports if r["control"])
    json.dump({"started": started, "ok": ok, "scenarios": [r["scenario"] for r in reports]}, open(f"{RESULTS}/summary.json", "w"), indent=2)
    print("CHAOS", "OK" if ok else "FAILED")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
