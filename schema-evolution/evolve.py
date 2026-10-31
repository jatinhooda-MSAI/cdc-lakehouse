import json
import os
import re
import time

import requests

from chaos import reconcile
from common import CONNECT, CONNECTOR, NAMESPACE, SR, TOPIC_PREFIX, catalog, log, pg, wait_for
from controller import resolve

L = log("evolve")
OUT = "/app/results/evolution.json"

STEPS = [
    {"name": "add nullable column", "table": "orders", "expect": "applied",
     "ddl": "ALTER TABLE orders ADD COLUMN priority smallint",
     "dml": "UPDATE orders SET priority = (id % 3)::smallint WHERE id IN (SELECT id FROM orders ORDER BY id DESC LIMIT 50)",
     "check": lambda s: "priority" in s},
    {"name": "add column with default", "table": "customers", "expect": "applied",
     "ddl": "ALTER TABLE customers ADD COLUMN loyalty_tier text NOT NULL DEFAULT 'bronze'",
     "dml": "UPDATE customers SET loyalty_tier = 'gold' WHERE id IN (SELECT id FROM customers ORDER BY id LIMIT 20)",
     "check": lambda s: "loyalty_tier" in s},
    {"name": "widen int to bigint", "table": "order_items", "expect": "applied",
     "ddl": "ALTER TABLE order_items ALTER COLUMN quantity TYPE bigint",
     "dml": "UPDATE order_items SET quantity = quantity + 1 WHERE id IN (SELECT id FROM order_items ORDER BY id DESC LIMIT 20)",
     "check": lambda s: s.get("quantity") == "long"},
    {"name": "narrow bigint to int", "table": "order_items", "expect": "rejected",
     "ddl": "ALTER TABLE order_items ALTER COLUMN quantity TYPE integer",
     "dml": "UPDATE order_items SET quantity = quantity + 1 WHERE id IN (SELECT id FROM order_items ORDER BY id DESC LIMIT 20)",
     "check": lambda s: s.get("quantity") == "long"},
    {"name": "rename column", "table": "customers", "expect": "halted", "resolve": ["rename", "full_name", "display_name"],
     "ddl": "ALTER TABLE customers RENAME COLUMN full_name TO display_name",
     "dml": "UPDATE customers SET display_name = display_name || ' jr' WHERE id IN (SELECT id FROM customers ORDER BY id LIMIT 20)",
     "check": lambda s: "display_name" in s and "full_name" not in s},
    {"name": "drop column", "table": "products", "expect": "halted", "resolve": ["drop", "description"],
     "ddl": "ALTER TABLE products DROP COLUMN description",
     "dml": "UPDATE products SET stock = stock + 1 WHERE id IN (SELECT id FROM products ORDER BY id LIMIT 20)",
     "check": lambda s: "description" not in s},
    {"name": "change numeric scale", "table": "payments", "expect": "halted",
     "ddl": "ALTER TABLE payments ALTER COLUMN amount TYPE numeric(12,4)",
     "dml": "UPDATE payments SET status = status WHERE id IN (SELECT id FROM payments ORDER BY id DESC LIMIT 20)",
     "check": lambda s: True},
]


def iceberg_columns(table):
    return {f.name: str(f.field_type) for f in catalog().load_table((NAMESPACE, table)).schema().fields}


def halted(table):
    return catalog().load_table((NAMESPACE, table)).properties.get("cdc.halted")


def submissions():
    text = requests.get("http://controller:8001/metrics", timeout=10).text
    return sum(float(m.group(1)) for m in re.finditer(r"^cdc_controller_job_submissions_total\{[^}]*\} (\S+)$", text, re.M))


def task_failure():
    st = requests.get(f"{CONNECT}/connectors/{CONNECTOR}/status", timeout=10).json()
    traces = [t.get("trace", "") for t in st["tasks"] if t["state"] == "FAILED"]
    return traces[0] if traces else None


def converged(table, timeout=600):
    def ok():
        r = reconcile.state(table)
        return r if r["ok"] else None
    return wait_for(ok, timeout, 10, f"{table} reconciled")


def run_step(step):
    L.info("--- %s: %s", step["name"], step["ddl"])
    t0 = time.time()
    before = submissions()
    with pg(admin=True) as c:
        c.execute(step["ddl"])
        c.execute(step["dml"])
    result = {"step": step["name"], "ddl": step["ddl"], "expected": step["expect"]}
    table = step["table"]
    if step["expect"] == "rejected":
        trace = wait_for(task_failure, 300, 5, "connector failure")
        result["observed"] = "rejected by schema registry" if "incompatible" in trace.lower() else "connector failed"
        result["reason"] = next((line.strip() for line in trace.splitlines() if "ncompatible" in line), trace.splitlines()[0])[:240]
        requests.put(f"{SR}/config/{TOPIC_PREFIX}{table}-value", json={"compatibility": "NONE"}, timeout=10).raise_for_status()
        requests.post(f"{CONNECT}/connectors/{CONNECTOR}/restart", params={"includeTasks": "true", "onlyFailed": "true"}, timeout=30)
        result["resolution"] = "compatibility NONE for this subject; the sink upcasts int into the long column"
    elif step["expect"] == "halted":
        reason = wait_for(lambda: halted(table) or task_failure(), 300, 5, "halt")
        result["observed"] = "halted" if halted(table) else "rejected by schema registry"
        result["reason"] = reason.splitlines()[0][:240]
        if "resolve" not in step:
            result["seconds"] = round(time.time() - t0, 1)
            result["final"] = "left halted: Iceberg cannot change a decimal scale; the table needs a rebuild"
            return result
        resolve.main([table] + step["resolve"])
        result["resolution"] = "make schema-resolve " + " ".join([table] + step["resolve"])
    else:
        result["observed"] = "applied"
    state = converged(table)
    columns = iceberg_columns(table)
    result.update(columns_ok=step["check"](columns), reconcile=state, job_restarts=int(submissions() - before),
                  seconds=round(time.time() - t0, 1))
    result["ok"] = result["columns_ok"] and state["ok"]
    L.info("--- %s: %s in %.0fs", step["name"], result["observed"], result["seconds"])
    return result


def main():
    results = [run_step(s) for s in STEPS]
    for r in results:
        print(f"{r['step']:<24} expected={r['expected']:<9} observed={r['observed']:<28} restarts={r.get('job_restarts', '-')} "
              f"rows_ok={r.get('reconcile', {}).get('ok', '-')} {r['seconds']}s")
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump(results, open(OUT, "w"), indent=2, default=str)


if __name__ == "__main__":
    main()
