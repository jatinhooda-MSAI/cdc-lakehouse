import argparse
import json
import os
import random
import statistics
import subprocess
import sys
import time

import requests

from common import FLINK, JOB_NAME, NAMESPACE, catalog, get, log, query, wait_for
from maintenance.maintain import cycle

L = log("bench-maintenance")
STATE = "/app/.state"
ARGS_FILE = "/app/flink/job.args"
QUERIES = {
    "count": "SELECT count(*) FROM {t}",
    "group_by_status": "SELECT status, count(*), sum(total) FROM {t} GROUP BY status",
    "point_lookup": "SELECT * FROM {t} WHERE id = {id}",
}


def flag(name, on):
    path = f"{STATE}/{name}"
    if on:
        open(path, "w").close()
    elif os.path.exists(path):
        os.remove(path)


def running():
    jobs = [j for j in get(f"{FLINK}/jobs/overview")["jobs"] if j["name"] == JOB_NAME and j["state"] == "RUNNING"]
    return jobs[0]["jid"] if jobs else None


def settled(jid):
    if any(v["status"] != "RUNNING" for v in get(f"{FLINK}/jobs/{jid}")["vertices"]):
        return False
    latest = get(f"{FLINK}/jobs/{jid}/checkpoints").get("latest") or {}
    return (latest.get("completed") or {}).get("latest_ack_timestamp", 0) > (latest.get("restored") or {}).get("restore_timestamp", 0)


def stop_with_savepoint():
    jid = wait_for(running, 300, 2, "running job")
    for attempt in range(5):
        wait_for(lambda: settled(jid), 300, 2, "job settled")
        r = requests.post(f"{FLINK}/jobs/{jid}/stop", json={"drain": False, "targetDirectory": "s3://flink/savepoints"}, timeout=30)
        r.raise_for_status()
        trigger = r.json()["request-id"]
        status = wait_for(lambda: (lambda s: s if s["status"]["id"] == "COMPLETED" else None)(
            get(f"{FLINK}/jobs/{jid}/savepoints/{trigger}")), 300, 2, "savepoint")
        if "failure-cause" not in status.get("operation", {}):
            wait_for(lambda: get(f"{FLINK}/jobs/{jid}")["state"] == "FINISHED", 300, 2, "job stopped")
            return jid, status["operation"]["location"]
        L.warning("savepoint attempt %d failed: %s", attempt + 1, str(status["operation"]["failure-cause"])[:200])
        time.sleep(10)
    raise RuntimeError("could not stop the job with a savepoint")


def restart(old):
    flag("supervisor-paused", False)
    wait_for(lambda: running() not in (None, old), 300, 2, "job resubmitted")


def make_shadow():
    cat = catalog()
    base = cat.load_table((NAMESPACE, "orders"))
    props = {k: v for k, v in base.properties.items() if k.startswith(("cdc.", "write.", "format", "commit."))}
    try:
        cat.drop_table((NAMESPACE, "orders_raw"))
    except Exception:
        pass
    cat.create_table((NAMESPACE, "orders_raw"), base.schema(), properties=props | {"cdc.shadow-of": "orders"})
    query("ALTER TABLE orders EXECUTE optimize")
    query("INSERT INTO orders_raw SELECT * FROM orders")


def flink_commits(table, since_ms):
    return query(f'SELECT count(*) FROM "{table}$snapshots" WHERE committed_at >= from_unixtime_nanos({since_ms} * 1000000) '
                 "AND element_at(summary, 'flink.max-committed-checkpoint-id') IS NOT NULL")[0][0]


def trino_up():
    wait_for(lambda: query("SELECT 1") == [[1]], 300, 5, "trino")


def measure(table, ids, runs=5):
    files = dict(query(f'SELECT content, count(*) FROM "{table}$files" GROUP BY content'))
    out = {"data_files": files.get(0, 0), "delete_files": files.get(1, 0) + files.get(2, 0),
           "snapshots": query(f'SELECT count(*) FROM "{table}$snapshots"')[0][0]}
    for name, sql in QUERIES.items():
        times = []
        for _ in range(runs):
            start = time.time()
            try:
                query(sql.format(t=table, id=random.choice(ids)))
                times.append(time.time() - start)
            except Exception as e:
                out["error"] = f"{name}: {str(e).splitlines()[0][:160]}"
                trino_up()
                return out
        out[name] = round(statistics.median(times) * 1000)
    return out


def teardown():
    flag("supervisor-paused", True)
    if os.path.exists(ARGS_FILE):
        os.remove(ARGS_FILE)
    old, savepoint = stop_with_savepoint()
    try:
        catalog().drop_table((NAMESPACE, "orders_raw"))
    except Exception:
        pass
    open(f"{STATE}/allow-non-restored", "a").write(savepoint + "\n")
    restart(old)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=60)
    ap.add_argument("--checkpoint-ms", type=int, default=5000)
    ap.add_argument("--rate", type=float, default=200)
    ap.add_argument("--every-commits", type=int, default=30)
    ap.add_argument("--maintain-every", type=int, default=120)
    ap.add_argument("--out", default="/app/results/maintenance.json")
    args = ap.parse_args()

    flag("maintenance-paused", True)
    flag("exporter-paused", True)
    if any(t[-1] == "orders_raw" for t in catalog().list_tables(NAMESPACE)):
        teardown()
    flag("supervisor-paused", True)
    open(ARGS_FILE, "w").write(f"--checkpoint-ms {args.checkpoint_ms}\n")
    old, _ = stop_with_savepoint()
    make_shadow()
    restart(old)
    ids = [r[0] for r in query("SELECT id FROM orders ORDER BY id DESC LIMIT 2000")]
    start = int(time.time() * 1000)
    gen = subprocess.Popen([sys.executable, "-m", "generator.generator", "run", "--rate", str(args.rate), "--seed", "41"])
    series, maintained_at, next_point, next_maint, raw_ok = [], [], 0, args.maintain_every, True
    try:
        deadline = time.time() + args.minutes * 60
        while time.time() < deadline:
            try:
                commits = flink_commits("orders", start)
                if commits >= next_maint:
                    cycle(["orders"], "30m", "1h")
                    maintained_at.append(commits)
                    next_maint += args.maintain_every
                if commits >= next_point:
                    point = {"commits": commits, "elapsed_s": round(time.time() - start / 1000), "maintained": measure("orders", ids)}
                    if raw_ok:
                        point["raw"] = measure("orders_raw", ids)
                        raw_ok = "error" not in point["raw"]
                    series.append(point)
                    L.info("%s", json.dumps(point))
                    next_point = commits + args.every_commits
            except Exception as e:
                L.warning("benchmark step failed: %s", str(e).splitlines()[0][:160])
                trino_up()
            time.sleep(5)
    finally:
        gen.terminate()
        gen.wait(120)
    result = {"checkpoint_ms": args.checkpoint_ms, "rate": args.rate, "minutes": args.minutes,
              "maintain_every_commits": args.maintain_every, "maintained_at": maintained_at,
              "rows_per_commit": round(query("SELECT count(*) FROM changelog WHERE table_name = 'orders' "
                                             f"AND kafka_ts >= from_unixtime_nanos({start} * 1000000)")[0][0] / max(1, flink_commits("orders", start))),
              "series": series}
    json.dump(result, open(args.out, "w"), indent=2)
    teardown()
    flag("maintenance-paused", False)
    flag("exporter-paused", False)


if __name__ == "__main__":
    main()
