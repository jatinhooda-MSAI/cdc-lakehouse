import os
import time

from prometheus_client import Gauge, start_http_server

from common import TABLES, log, query

L = log("exporter")
WATCHED = TABLES + ["debezium_heartbeat"]
CONTENT = {0: "data", 1: "position_deletes", 2: "equality_deletes"}

FRESHNESS = Gauge("lakehouse_freshness_seconds", "now minus newest source commit visible in Iceberg", ["table"])
FILES = Gauge("lakehouse_files", "live files in the current snapshot", ["table", "content"])
FILE_BYTES = Gauge("lakehouse_file_bytes", "bytes in the current snapshot", ["table", "content"])
SNAPSHOTS = Gauge("lakehouse_snapshots", "snapshots kept in table metadata", ["table"])
SCAN = Gauge("lakehouse_scan_seconds", "time for SELECT count(*) on the table", ["table"])


def collect(t):
    lag = query(f"SELECT to_unixtime(current_timestamp) - to_unixtime(max(_source_ts)) FROM {t}")[0][0]
    if lag is not None:
        FRESHNESS.labels(t).set(lag)
    seen = set()
    for content, n, size in query(f'SELECT content, count(*), sum(file_size_in_bytes) FROM "{t}$files" GROUP BY content'):
        FILES.labels(t, CONTENT[content]).set(n)
        FILE_BYTES.labels(t, CONTENT[content]).set(size)
        seen.add(content)
    for content in set(CONTENT) - seen:
        FILES.labels(t, CONTENT[content]).set(0)
        FILE_BYTES.labels(t, CONTENT[content]).set(0)
    SNAPSHOTS.labels(t).set(query(f'SELECT count(*) FROM "{t}$snapshots"')[0][0])
    start = time.time()
    query(f"SELECT count(*) FROM {t}")
    SCAN.labels(t).set(time.time() - start)


def main():
    start_http_server(8000)
    while True:
        if os.path.exists("/app/.state/exporter-paused"):
            time.sleep(30)
            continue
        for t in WATCHED:
            try:
                collect(t)
            except Exception as e:
                L.warning("collect %s failed: %s", t, str(e).splitlines()[0][:160])
        time.sleep(30)


if __name__ == "__main__":
    main()
