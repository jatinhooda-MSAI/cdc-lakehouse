import argparse
import json
import os
import subprocess
import sys
import time

from bench.latency import summarize
from bench.maintenance import ARGS_FILE, flag, restart, stop_with_savepoint
from common import log

L = log("sweep")


def run_interval(ms, seconds, rate):
    flag("supervisor-paused", True)
    open(ARGS_FILE, "w").write(f"--checkpoint-ms {ms}\n")
    restart(stop_with_savepoint()[0])
    time.sleep(60)
    since = int(time.time() * 1000)
    subprocess.run([sys.executable, "-m", "generator.generator", "run", "--rate", str(rate), "--probe",
                    "--duration", str(seconds), "--seed", str(50 + ms // 1000)], check=True)
    until = int(time.time() * 1000)
    time.sleep(ms / 1000 * 3 + 30)
    return {"checkpoint_ms": ms, "rate": rate, "seconds": seconds} | summarize(since, until)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--intervals", nargs="*", type=int, default=[10000, 30000, 60000])
    ap.add_argument("--seconds", type=int, default=600)
    ap.add_argument("--rate", type=float, default=100)
    ap.add_argument("--out", default="/app/results/latency.json")
    args = ap.parse_args()
    flag("maintenance-paused", True)
    results = []
    try:
        for ms in args.intervals:
            results.append(run_interval(ms, args.seconds, args.rate))
            L.info("%s", json.dumps(results[-1]))
            json.dump(results, open(args.out, "w"), indent=2)
    finally:
        flag("supervisor-paused", True)
        if os.path.exists(ARGS_FILE):
            os.remove(ARGS_FILE)
        restart(stop_with_savepoint()[0])
        flag("maintenance-paused", False)


if __name__ == "__main__":
    main()
