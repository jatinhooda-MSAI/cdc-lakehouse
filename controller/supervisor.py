import os
import time

import requests
from prometheus_client import Counter, Gauge

from common import CONNECT, FLINK, JOB_NAME, NAMESPACE, catalog, log, s3

L = log("supervisor")
JAR = "/app/flink/job/target/cdc-job.jar"
ARGS_FILE = "/app/flink/job.args"
ACTIVE = {"CREATED", "INITIALIZING", "RUNNING", "RESTARTING", "RECONCILING", "FAILING", "CANCELLING"}
SUBMITS = Counter("cdc_controller_job_submissions_total", "Flink job submissions", ["restored"])
RESTARTS = Counter("cdc_controller_connector_restarts_total", "Failed connector or task restarts", ["connector"])
TASK_FAILED = Gauge("cdc_controller_connector_failed", "1 while a connector or task is FAILED", ["connector"])
JOB_UP = Gauge("cdc_controller_job_running", "1 while the CDC job is RUNNING")

BLOCKED = Gauge("cdc_controller_blocked", "1 while the job cannot be started safely")
state = {"last_submit": 0.0, "last_change": 0.0, "last_drift": None, "backoff": {}, "seen_job": False, "lost_at": None}


def jobs():
    return requests.get(f"{FLINK}/jobs/overview", timeout=10).json()["jobs"]


def current():
    mine = [j for j in jobs() if j["name"] == JOB_NAME]
    active = [j for j in mine if j["state"] in ACTIVE]
    if active:
        return active[0]
    return max(mine, key=lambda j: j["start-time"]) if mine else None


def failure(jid):
    ex = requests.get(f"{FLINK}/jobs/{jid}/exceptions", timeout=10).json()
    entries = ex.get("exceptionHistory", {}).get("entries", [])
    return ex.get("root-exception") or (entries[0].get("stacktrace") if entries else "") or ""


def halt_reason(trace):
    for line in trace.splitlines():
        if "HaltException:" in line:
            return line.split("HaltException:", 1)[1].strip()
    return None


def restore_point():
    client = s3()
    best = None
    for prefix in ("checkpoints/", "savepoints/"):
        for page in client.get_paginator("list_objects_v2").paginate(Bucket="flink", Prefix=prefix):
            for o in page.get("Contents", []):
                if o["Key"].endswith("/_metadata") and (best is None or o["LastModified"] > best["LastModified"]):
                    best = o
    return f"s3://flink/{best['Key'].rsplit('/', 1)[0]}" if best else None


def job_args():
    if os.path.exists(ARGS_FILE):
        return open(ARGS_FILE).read().split()
    return []


def submit(restore):
    with open(JAR, "rb") as f:
        up = requests.post(f"{FLINK}/jars/upload", files={"jarfile": ("cdc-job.jar", f, "application/x-java-archive")}, timeout=120)
    up.raise_for_status()
    jar = up.json()["filename"].split("/")[-1]
    body = {"entryClass": "lakehouse.cdc.CdcJob", "programArgsList": job_args()}
    if restore:
        body.update(savepointPath=restore, allowNonRestoredState=os.path.exists("/app/.state/allow-non-restored"))
    try:
        r = requests.post(f"{FLINK}/jars/{jar}/run", json=body, timeout=300)
        r.raise_for_status()
    finally:
        requests.delete(f"{FLINK}/jars/{jar}", timeout=30)
    SUBMITS.labels(str(bool(restore)).lower()).inc()
    L.info("submitted job %s restore=%s args=%s", r.json()["jobid"], restore, job_args())
    return r.json()["jobid"]


def cancel(jid):
    requests.patch(f"{FLINK}/jobs/{jid}", params={"mode": "cancel"}, timeout=30)


def stop_with_savepoint(jid):
    r = requests.post(f"{FLINK}/jobs/{jid}/stop", json={"drain": False, "targetDirectory": "s3://flink/savepoints"}, timeout=30)
    r.raise_for_status()
    return r.json()["request-id"]


def mark_halted(reason):
    table = reason.split("table=", 1)[1].split()[0] if "table=" in reason else None
    if table:
        tbl = catalog().load_table((NAMESPACE, table))
        with tbl.transaction() as tx:
            tx.set_properties({"cdc.halted": reason})
    L.error("HALT job: %s", reason)


def tick(halted, created):
    if os.path.exists("/app/.state/supervisor-paused"):
        return
    job = current()
    if created:
        state["last_change"] = time.time()
        if job and job["state"] == "RUNNING":
            L.info("new tables %s, restarting job through a savepoint", created)
            stop_with_savepoint(job["jid"])
            return
    JOB_UP.set(1 if job and job["state"] == "RUNNING" else 0)
    if halted:
        if job and job["state"] in ACTIVE and job["state"] != "CANCELLING":
            L.error("cancelling job, waiting for operator: %s", halted)
            cancel(job["jid"])
        return
    if job and job["state"] in ACTIVE:
        state["seen_job"], state["lost_at"] = True, None
        return
    tables = [t for t in catalog().list_tables(NAMESPACE) if t[-1] != "changelog"]
    if not tables:
        return
    if job is None and state["seen_job"]:
        state["lost_at"] = state["lost_at"] or time.time()
        if time.time() - state["lost_at"] < 30:
            return
    if time.time() - max(state["last_submit"], state["last_change"]) < 20:
        return
    restore = restore_point()
    if restore is None and any(catalog().load_table(t).current_snapshot() for t in tables):
        BLOCKED.set(1)
        L.error("no checkpoint or savepoint but tables are not empty; replaying from earliest would resurrect deletes")
        return
    BLOCKED.set(0)
    if job and job["state"] == "FAILED":
        reason = halt_reason(failure(job["jid"]))
        if reason and not reason.startswith("schema-drift"):
            return mark_halted(reason)
        if reason and reason == state["last_drift"]:
            return mark_halted(f"unresolved {reason}")
        state["last_drift"] = reason
        L.warning("job %s failed: %s", job["jid"], reason or "see Flink UI")
    state["last_submit"] = time.time()
    submit(restore)


def connect_tick():
    for name in requests.get(f"{CONNECT}/connectors", timeout=10).json():
        st = requests.get(f"{CONNECT}/connectors/{name}/status", timeout=10).json()
        failed = [t for t in st.get("tasks", []) if t["state"] == "FAILED"]
        down = st["connector"]["state"] == "FAILED" or bool(failed)
        TASK_FAILED.labels(name).set(1 if down else 0)
        if not down:
            continue
        n, next_at = state["backoff"].get(name, (0, 0.0))
        if time.time() < next_at:
            continue
        trace = (failed[0].get("trace") if failed else st["connector"].get("trace")) or ""
        L.warning("connector %s failed, restart #%d: %s", name, n + 1, trace.splitlines()[0] if trace else "")
        requests.post(f"{CONNECT}/connectors/{name}/restart", params={"includeTasks": "true", "onlyFailed": "true"}, timeout=30)
        RESTARTS.labels(name).inc()
        state["backoff"][name] = (n + 1, time.time() + min(300, 10 * 2 ** n))
