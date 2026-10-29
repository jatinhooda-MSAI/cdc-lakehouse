import argparse
import json
import statistics
import subprocess
import sys
import time

from common import log, pg, query

L = log("latency")


def percentile(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else None


def visibility(since_ms):
    with pg(admin=True) as conn:
        commits = dict(conn.execute(
            "SELECT id, (extract(epoch FROM pg_xact_commit_timestamp(xmin)) * 1000)::bigint FROM latency_probe "
            "WHERE created_at >= to_timestamp(%s / 1000.0)", (since_ms,)).fetchall())
    if not commits:
        return {}, {}
    low = min(commits)
    snaps = query('SELECT snapshot_id, to_unixtime(committed_at) * 1000 FROM "latency_probe$snapshots" '
                  f"WHERE committed_at >= from_unixtime_nanos({since_ms} * 1000000) ORDER BY committed_at")
    seen = {}
    for sid, ts in snaps:
        for (pid,) in query(f"SELECT id FROM latency_probe FOR VERSION AS OF {sid} WHERE id >= {low}"):
            seen.setdefault(pid, ts)
    kafka = dict(query(f"SELECT CAST(pk AS bigint), to_unixtime(min(kafka_ts)) * 1000 FROM changelog "
                       f"WHERE table_name = 'latency_probe' AND CAST(pk AS bigint) >= {low} GROUP BY 1"))
    return commits, {pid: (seen.get(pid), kafka.get(pid)) for pid in commits}


def summarize(since_ms, until_ms=None):
    commits, vis = visibility(since_ms)
    rows = [(commits[p], v, k) for p, (v, k) in vis.items() if v is not None and (until_ms is None or commits[p] <= until_ms)]
    e2e = [v - c for c, v, _ in rows]
    capture = [k - c for c, _, k in rows if k is not None]
    sink = [v - k for _, v, k in rows if k is not None]
    stat = lambda xs: {"p50": percentile(xs, 50), "p95": percentile(xs, 95), "p99": percentile(xs, 99), "max": max(xs) if xs else None,
                       "mean": round(statistics.mean(xs)) if xs else None}
    return {"samples": len(e2e), "not_visible": sum(1 for v, _ in vis.values() if v is None),
            "e2e_ms": stat(e2e), "capture_ms": stat(capture), "sink_ms": stat(sink)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=100)
    ap.add_argument("--seconds", type=int, default=600)
    ap.add_argument("--label", default="")
    ap.add_argument("--out")
    args = ap.parse_args()
    since = int(time.time() * 1000)
    gen = subprocess.Popen([sys.executable, "-m", "generator.generator", "run", "--rate", str(args.rate), "--probe",
                            "--duration", str(args.seconds), "--seed", "23"])
    gen.wait()
    until = int(time.time() * 1000)
    time.sleep(90)
    result = {"label": args.label, "rate": args.rate, "seconds": args.seconds} | summarize(since, until)
    print(json.dumps(result, indent=2))
    if args.out:
        json.dump(result, open(args.out, "w"), indent=2)


if __name__ == "__main__":
    main()
