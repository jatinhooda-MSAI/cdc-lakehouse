import argparse
import os
import time

from prometheus_client import Counter, Gauge, start_http_server

from common import FLINK, JOB_NAME, TABLES, get, log, query

L = log("maintenance")
PAUSE = "/app/.state/maintenance-paused"
TARGETS = TABLES + ["changelog", "latency_probe", "debezium_heartbeat"]
RUNS = Counter("lakehouse_maintenance_runs_total", "Maintenance statements", ["table", "task", "result"])
SECONDS = Gauge("lakehouse_maintenance_seconds", "Duration of the last maintenance statement", ["table", "task"])


def job_settled():
    jobs = [j for j in get(f"{FLINK}/jobs/overview")["jobs"] if j["name"] == JOB_NAME and j["state"] == "RUNNING"]
    if not jobs:
        return False
    latest = get(f"{FLINK}/jobs/{jobs[0]['jid']}/checkpoints").get("latest") or {}
    done = (latest.get("completed") or {}).get("latest_ack_timestamp", 0)
    restored = (latest.get("restored") or {}).get("restore_timestamp", 0)
    return done > restored


def task(table, name, sql):
    start = time.time()
    try:
        query(sql)
        RUNS.labels(table, name, "ok").inc()
        ok = True
    except Exception as e:
        RUNS.labels(table, name, "failed").inc()
        L.warning("%s %s failed: %s", name, table, str(e).splitlines()[0][:200])
        ok = False
    SECONDS.labels(table, name).set(time.time() - start)
    return ok


def cycle(tables, retention, orphans, expire=True):
    results = {}
    for t in tables:
        results[t] = {"optimize": task(t, "optimize", f"ALTER TABLE {t} EXECUTE optimize"),
                      "optimize_manifests": task(t, "optimize_manifests", f"ALTER TABLE {t} EXECUTE optimize_manifests")}
        if expire and job_settled():
            results[t]["expire_snapshots"] = task(
                t, "expire_snapshots", f"ALTER TABLE {t} EXECUTE expire_snapshots(retention_threshold => '{retention}')")
            results[t]["remove_orphan_files"] = task(
                t, "remove_orphan_files", f"ALTER TABLE {t} EXECUTE remove_orphan_files(retention_threshold => '{orphans}')")
    L.info("maintenance cycle: %s", results)
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--tables", nargs="*", default=TARGETS)
    ap.add_argument("--interval", type=int, default=int(os.environ.get("MAINTENANCE_INTERVAL_S", "3600")))
    ap.add_argument("--retention", default=os.environ.get("SNAPSHOT_RETENTION", "24h"))
    ap.add_argument("--orphans", default=os.environ.get("ORPHAN_RETENTION", "24h"))
    args = ap.parse_args()
    if args.once:
        cycle(args.tables, args.retention, args.orphans)
        return
    start_http_server(8002)
    next_run = time.time() + args.interval
    while True:
        if time.time() >= next_run:
            next_run += args.interval
            if not os.path.exists(PAUSE):
                cycle(args.tables, args.retention, args.orphans)
        time.sleep(10)


if __name__ == "__main__":
    main()
