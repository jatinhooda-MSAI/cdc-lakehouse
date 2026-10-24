import time

import requests
from confluent_kafka.admin import AdminClient

from common import CATALOG, CONNECT, FLINK, KAFKA, SR, log, pg, query, s3, wait_for

L = log("wait")

CHECKS = {
    "postgres": lambda: pg().execute("SELECT 1").fetchone(),
    "kafka": lambda: AdminClient({"bootstrap.servers": KAFKA}).list_topics(timeout=5).brokers,
    "schema-registry": lambda: requests.get(f"{SR}/subjects", timeout=5).ok,
    "connect": lambda: requests.get(f"{CONNECT}/connectors", timeout=5).ok,
    "minio": lambda: all(s3().head_bucket(Bucket=b) for b in ("warehouse", "flink")),
    "iceberg-rest": lambda: requests.get(f"{CATALOG}/v1/config", timeout=5).ok,
    "flink": lambda: requests.get(f"{FLINK}/overview", timeout=5).json()["slots-total"] >= 4,
    "trino": lambda: query("SELECT 1") == [[1]],
}


def main():
    start = time.time()
    for name, check in CHECKS.items():
        wait_for(check, 300, what=name)
        L.info("%s ready after %.0fs", name, time.time() - start)


if __name__ == "__main__":
    main()
