import time

import requests

from common import FLINK, JOB_NAME, get, log, wait_for

L = log("upgrade")


def running():
    jobs = [j for j in get(f"{FLINK}/jobs/overview")["jobs"] if j["name"] == JOB_NAME and j["state"] == "RUNNING"]
    return jobs[0]["jid"] if jobs else None


def main():
    old = running()
    r = requests.post(f"{FLINK}/jobs/{old}/stop", json={"drain": False, "targetDirectory": "s3://flink/savepoints"}, timeout=30)
    r.raise_for_status()
    trigger = r.json()["request-id"]
    status = wait_for(lambda: get(f"{FLINK}/jobs/{old}/savepoints/{trigger}")["status"]["id"] == "COMPLETED"
                      and get(f"{FLINK}/jobs/{old}/savepoints/{trigger}"), 300, what="savepoint")
    path = status["operation"].get("location")
    L.info("stopped %s with savepoint %s", old, path)
    start = time.time()
    new = wait_for(lambda: (running() or None) if running() != old else None, 300, what="resubmitted job")
    L.info("controller resubmitted %s from the savepoint after %.0fs", new, time.time() - start)


if __name__ == "__main__":
    main()
