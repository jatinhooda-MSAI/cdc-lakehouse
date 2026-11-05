import glob
import json
import os
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = "/app"
RESULTS = f"{ROOT}/results"
SURFACE, INK, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3de"
BLUE, ORANGE = "#2a78d6", "#eb6834"
ORDER = ["tm-kill-during-checkpoint", "tm-kill-before-iceberg-commit", "jobmanager-kill", "kafka-broker-kill", "connect-sigkill",
         "minio-pause-150s", "replay-old-offsets", "poison-record", "compaction-under-load", "control-replay-without-guard"]


def load(path):
    return json.load(open(path)) if os.path.exists(path) else None


FAULT = {"tm-kill-during-checkpoint": 5, "tm-kill-before-iceberg-commit": 3, "jobmanager-kill": 10, "kafka-broker-kill": 60,
         "connect-sigkill": 10, "minio-pause-150s": 150}


def secs(x):
    return f"{x:.0f} s" if x else "-"


def evidence(r):
    ev = r["evidence"]
    name = r["scenario"]
    if name == "tm-kill-during-checkpoint":
        return "; ".join(f"chk {e['killed_during_checkpoint']} {str(e['that_checkpoint']).lower()}, restored {e['restored_from']}" for e in ev)
    if name == "tm-kill-before-iceberg-commit":
        return "; ".join(f"chk {e['pending_checkpoint']} committed {max(e['snapshots_for_checkpoint'].values())}x" for e in ev)
    if name == "jobmanager-kill":
        return f"resumed from {ev[0]['restored_from'].rsplit('/', 2)[-1] if ev[0].get('restored_from') else '?'}"
    if name == "kafka-broker-kill":
        return f"{r['changelog']['redelivered_events']} events re-sent after the outage"
    if name == "connect-sigkill":
        return f"{r['changelog']['redelivered_events'] - ev[0]['redelivered_before']} events re-sent"
    if name == "minio-pause-150s":
        return f"{'commits continued' if ev[0]['committed_during_pause'] else 'no commits'} during pause, {ev[0]['failed_checkpoints']} checkpoint(s) failed"
    if name in ("replay-old-offsets", "control-replay-without-guard"):
        return f"{ev[0]['replayed']} old records re-produced, {ev[0]['stale_dropped']} dropped"
    if name == "poison-record":
        return f"{ev[0]['dlq_records']} record in DLQ"
    if name == "compaction-under-load":
        return ", ".join(f"{o['table']} {'ok' if o['ok'] else 'failed'}" for o in ev[0]["optimize"])
    return ""


def chaos_table():
    rows = []
    for name in ORDER:
        r = load(f"{RESULTS}/chaos/{name}.json")
        if not r:
            continue
        fault = FAULT.get(name, 0)
        fresh = max(e.get("to_caught_up_s", 0) - e.get("fault_s", fault) for e in r["evidence"]) if fault else 0
        verdict = "pass" if r["ok"] else "FAIL"
        if r["control"]:
            rep = r.get("repaired", {})
            verdict = (f"fails as expected; incremental snapshot left {rep.get('diffs_after_snapshot', '?')} diffs, "
                       f"deleting {rep.get('zombies_deleted', '?')} resurrected keys left {rep.get('diffs', '?')}") if not r["ok"] else "control did not fail"
        lat = r["latency"]["e2e_ms"]["max"]
        rows.append(f"| {name} | {r['runs']} | {secs(fault)} | {evidence(r)} | {r['diffs']} | {r['monotonic']['regressions']} | "
                    f"{secs(max(fresh, 0))} | {secs(lat / 1000 if lat else None)} | {verdict} |")
    head = ("| scenario | runs | fault held | evidence the fault landed | row diffs | version regressions | "
            "fresh data after heal | worst latency | result |\n|---|---|---|---|---|---|---|---|---|")
    return head + "\n" + "\n".join(x for x in rows if x)


def latency_table():
    data = load(f"{RESULTS}/latency.json") or []
    rows = [f"| {d['checkpoint_ms'] // 1000} s | {d['samples']} | {d['e2e_ms']['p50'] / 1000:.1f} s | {d['e2e_ms']['p95'] / 1000:.1f} s | "
            f"{d['e2e_ms']['p99'] / 1000:.1f} s | {d['capture_ms']['p50'] / 1000:.2f} s | {d['sink_ms']['p50'] / 1000:.1f} s |" for d in data]
    return ("| checkpoint | probes | p50 | p95 | p99 | Postgres to Kafka p50 | Kafka to Iceberg p50 |\n|---|---|---|---|---|---|---|\n"
            + "\n".join(rows))


def evolution_table():
    data = load(f"{RESULTS}/evolution.json") or []
    rows = []
    for d in data:
        fix = d.get("final") if d.get("resolution", "-") == "-" and d.get("final") else d.get("resolution", "-")
        rows.append(f"| `{d['ddl']}` | {d['expected']} | {d['observed']} | {fix} | "
                    f"{d.get('job_restarts', '-')} | {'yes' if d.get('reconcile', {}).get('ok') else '-'} |")
    return "| DDL | expected | observed | resolution | job restarts | rows match |\n|---|---|---|---|---|---|\n" + "\n".join(rows)


def maintenance_chart():
    data = load(f"{RESULTS}/maintenance.json")
    if not data:
        return ""
    s = data["series"]
    x = [p["commits"] for p in s]
    panels = [("group_by_status", "GROUP BY status (ms)"), ("count", "count(*) (ms)"), ("delete_files", "delete files")]
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.6), dpi=150, facecolor=SURFACE)
    for ax, (key, title) in zip(axes, panels):
        ax.set_facecolor(SURFACE)
        for arm, color, label in (("raw", ORANGE, "never maintained"), ("maintained", BLUE, "maintained")):
            y = [p[arm].get(key) for p in s]
            ax.plot(x, y, color=color, linewidth=2, marker="o", markersize=4, label=label)
            last = next(((xi, yi) for xi, yi in zip(reversed(x), reversed(y)) if yi is not None), None)
            if last:
                ax.annotate(label, last, xytext=(4, 0), textcoords="offset points", color=MUTED, fontsize=8, va="center")
        for m in data["maintained_at"]:
            ax.axvline(m, color=GRID, linewidth=1, zorder=0)
        ax.set_title(title, color=INK, fontsize=10, loc="left")
        ax.tick_params(colors=MUTED, labelsize=8)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(GRID)
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_xlabel(f"Flink commits ({data['checkpoint_ms'] // 1000} s checkpoints)", color=MUTED, fontsize=8)
        ax.set_ylim(bottom=0)
    axes[0].legend(frameon=False, fontsize=8, labelcolor=INK, loc="upper left")
    fig.tight_layout()
    fig.savefig(f"{RESULTS}/maintenance.png", facecolor=SURFACE)
    first, last = s[0], s[-1]
    after = next(p for p in reversed(s) if p["commits"] in data["maintained_at"])
    rows = [f"| {label} | {first['raw'].get(k)} | {last['raw'].get(k)} | {after['maintained'].get(k)} | {last['maintained'].get(k)} |"
            for k, label in (("group_by_status", "GROUP BY status, ms"), ("count", "count(*), ms"), ("point_lookup", "point lookup, ms"),
                             ("data_files", "data files"), ("delete_files", "delete files"))]
    return (f"![query latency and delete files vs commits](results/maintenance.png)\n\n"
            f"| | start | never maintained, {last['commits']} commits | maintained, right after the run at {after['commits']} | "
            f"maintained, {last['commits']} commits |\n|---|---|---|---|---|\n" + "\n".join(rows))


def footprint_table():
    data = load(f"{RESULTS}/chaos/footprint.json")
    if not data:
        return ""
    rows = [f"| {k} | {v['peak_mib']:.0f} MiB | {v['limit_mib']:.0f} MiB |" for k, v in sorted(data.items(), key=lambda kv: -kv[1]["peak_mib"])]
    total = sum(v["peak_mib"] for v in data.values())
    return "| container | peak during chaos | limit |\n|---|---|---|\n" + "\n".join(rows) + f"\n| total | {total / 1024:.1f} GiB | |"


def splice(text, tag, body):
    return re.sub(rf"(<!-- {tag} -->\n).*?(<!-- /{tag} -->)", lambda m: m.group(1) + body + "\n" + m.group(2), text, flags=re.S)


def main():
    path = f"{ROOT}/README.md"
    text = open(path).read()
    for tag, fn in (("chaos", chaos_table), ("latency", latency_table), ("evolution", evolution_table),
                    ("maintenance", maintenance_chart), ("footprint", footprint_table)):
        body = fn()
        if body:
            text = splice(text, tag, body)
    open(path, "w").write(text)
    print("README tables updated from", ", ".join(sorted(os.path.relpath(p, ROOT) for p in glob.glob(f"{RESULTS}/**/*.json", recursive=True))))


if __name__ == "__main__":
    main()
