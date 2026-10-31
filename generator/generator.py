import argparse
import json
import random
import signal
import sqlite3
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from decimal import Decimal

from psycopg.rows import dict_row

from common import log, pg

L = log("generator")
LEDGER = "/app/.state/ledger.sqlite"
NEXT = {"created": "paid", "paid": "shipped", "shipped": "delivered"}
COUNTRIES = ["US", "GB", "DE", "IN", "BR", "JP", "FR", "CA"]
METHODS = ["card", "paypal", "bank"]
WEIGHTS = {
    "new_customer": 5, "new_order": 25, "advance_order": 40, "cancel_order": 3, "delete_order": 4,
    "update_product": 10, "toast_product": 1, "update_customer": 5, "pk_update_payment": 1, "type_sample": 1,
}


def encode(v):
    if isinstance(v, (bytes, memoryview)):
        return bytes(v).hex()
    if isinstance(v, (datetime, date, dtime)):
        return v.isoformat()
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True)
    return str(v)


class Ledger:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS ops (seq INTEGER PRIMARY KEY, ts REAL, tbl TEXT, pk INTEGER, op TEXT, row TEXT)")
        self.pending = []

    def add(self, tbl, op, row):
        body = None if op == "d" else json.dumps(row, default=encode, sort_keys=True)
        self.pending.append((time.time(), tbl, row["id"], op, body))

    def flush(self):
        if self.pending:
            self.db.executemany("INSERT INTO ops (ts, tbl, pk, op, row) VALUES (?, ?, ?, ?, ?)", self.pending)
            self.db.commit()
            self.pending = []


class Workload:
    def __init__(self, rng, ledger):
        self.rng = rng
        self.ledger = ledger
        self.conn = pg(autocommit=False)
        self.conn.row_factory = dict_row
        self.counts = {}
        q = lambda sql: [r["id"] for r in self.conn.execute(sql).fetchall()]
        self.customers = q("SELECT id FROM customers ORDER BY id")
        self.products = q("SELECT id FROM products ORDER BY id")
        self.orders = q("SELECT id FROM orders ORDER BY id")
        self.payments = q("SELECT id FROM payments ORDER BY id")
        self.samples = q("SELECT id FROM type_samples ORDER BY id")
        self.active = {r["id"]: r["status"] for r in self.conn.execute(
            "SELECT id, status FROM orders WHERE status IN ('created', 'paid', 'shipped') ORDER BY id").fetchall()}
        self.conn.commit()
        self.hot = self.products[:3]

    def one(self, sql, params=()):
        return self.conn.execute(sql, params).fetchone()

    def all(self, sql, params=()):
        return self.conn.execute(sql, params).fetchall()

    def rec(self, tbl, op, rows):
        for r in rows if isinstance(rows, list) else [rows]:
            self.ledger.add(tbl, op, r)

    def step(self, name=None):
        name = name or self.rng.choices(list(WEIGHTS), weights=list(WEIGHTS.values()))[0]
        try:
            getattr(self, name)()
            self.conn.commit()
            self.counts[name] = self.counts.get(name, 0) + 1
        except Exception:
            self.conn.rollback()
            self.ledger.pending.clear()
            raise
        self.ledger.flush()

    def customer_row(self):
        return (f"{uuid.uuid4()}@example.com",
                f"Customer {self.rng.randint(1, 10**6)}", self.rng.choice(COUNTRIES))

    def new_customer(self):
        row = self.one("INSERT INTO customers (email, full_name, country) VALUES (%s, %s, %s) RETURNING *", self.customer_row())
        self.rec("customers", "c", row)
        self.customers.append(row["id"])

    def new_order(self):
        order = self.one("INSERT INTO orders (customer_id, status) VALUES (%s, 'created') RETURNING *", (self.rng.choice(self.customers),))
        self.rec("orders", "c", order)
        total = Decimal("0")
        for pid in self.rng.sample(self.products, self.rng.randint(1, 4)):
            item = self.one("INSERT INTO order_items (order_id, product_id, quantity, unit_price) "
                            "SELECT %s, id, %s, price FROM products WHERE id = %s RETURNING *",
                            (order["id"], self.rng.randint(1, 5), pid))
            self.rec("order_items", "c", item)
            total += item["quantity"] * item["unit_price"]
        self.rec("orders", "u", self.one("UPDATE orders SET total = %s WHERE id = %s RETURNING *", (total, order["id"])))
        self.orders.append(order["id"])
        self.active[order["id"]] = "created"

    def advance_order(self, oid=None):
        if oid is None:
            if not self.active:
                return self.new_order()
            oid = self.rng.choice(list(self.active))
        status = NEXT[self.active[oid]]
        row = self.one("UPDATE orders SET status = %s WHERE id = %s RETURNING *", (status, oid))
        self.rec("orders", "u", row)
        if status == "paid":
            pay = self.one("INSERT INTO payments (order_id, amount, method, status) VALUES (%s, %s, %s, 'captured') RETURNING *",
                           (oid, row["total"], self.rng.choice(METHODS)))
            self.rec("payments", "c", pay)
            self.payments.append(pay["id"])
        if status == "delivered":
            self.rec("payments", "u", self.all("UPDATE payments SET status = 'settled' WHERE order_id = %s RETURNING *", (oid,)))
            del self.active[oid]
        else:
            self.active[oid] = status

    def cancel_order(self):
        open_orders = [o for o, s in self.active.items() if s in ("created", "paid")]
        if not open_orders:
            return
        oid = self.rng.choice(open_orders)
        self.rec("orders", "u", self.one("UPDATE orders SET status = 'cancelled' WHERE id = %s RETURNING *", (oid,)))
        self.rec("payments", "u", self.all("UPDATE payments SET status = 'refunded' WHERE order_id = %s RETURNING *", (oid,)))
        del self.active[oid]

    def delete_order(self):
        if len(self.orders) < 10:
            return
        oid = self.orders.pop(self.rng.randrange(len(self.orders)))
        self.rec("order_items", "d", self.all("DELETE FROM order_items WHERE order_id = %s RETURNING id", (oid,)))
        gone = self.all("DELETE FROM payments WHERE order_id = %s RETURNING id", (oid,))
        self.rec("payments", "d", gone)
        self.payments = [p for p in self.payments if p not in {g["id"] for g in gone}] if gone else self.payments
        self.rec("orders", "d", self.all("DELETE FROM orders WHERE id = %s RETURNING id", (oid,)))
        self.active.pop(oid, None)

    def update_product(self):
        pid = self.rng.choice(self.hot) if self.rng.random() < 0.3 else self.rng.choice(self.products)
        row = self.one("UPDATE products SET price = round(price * %s, 2), stock = greatest(stock + %s, 0) WHERE id = %s RETURNING *",
                       (Decimal(self.rng.choice(["0.97", "1.00", "1.03"])), self.rng.randint(-5, 5), pid))
        self.rec("products", "u", row)

    def toast_product(self):
        pid = self.rng.choice(self.products)
        self.rec("products", "u", self.one("UPDATE products SET description = %s WHERE id = %s RETURNING *", (self.long_text(), pid)))

    def update_customer(self):
        cid = self.rng.choice(self.customers)
        email, name, _ = self.customer_row()
        self.rec("customers", "u", self.one("UPDATE customers SET email = %s, full_name = %s WHERE id = %s RETURNING *", (email, name, cid)))

    def pk_update_payment(self):
        if not self.payments:
            return
        old = self.rng.choice(self.payments)
        row = self.one("UPDATE payments SET id = nextval('payments_id_seq') WHERE id = %s RETURNING *", (old,))
        self.ledger.add("payments", "d", {"id": old})
        self.rec("payments", "c", row)
        self.payments[self.payments.index(old)] = row["id"]

    def type_sample(self):
        values = self.sample_values()
        if self.samples and self.rng.random() < 0.5:
            sid = self.rng.choice(self.samples)
            row = self.one("UPDATE type_samples SET c_smallint=%s, c_int=%s, c_bigint=%s, c_numeric=%s, c_real=%s, c_double=%s, "
                           "c_bool=%s, c_text=%s, c_varchar=%s, c_char=%s, c_date=%s, c_time=%s, c_ts=%s, c_tstz=%s, c_uuid=%s, "
                           "c_jsonb=%s, c_bytea=%s WHERE id = %s RETURNING *", values + (sid,))
            self.rec("type_samples", "u", row)
        else:
            row = self.one("INSERT INTO type_samples (c_smallint, c_int, c_bigint, c_numeric, c_real, c_double, c_bool, c_text, "
                           "c_varchar, c_char, c_date, c_time, c_ts, c_tstz, c_uuid, c_jsonb, c_bytea) VALUES "
                           "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *", values)
            self.rec("type_samples", "c", row)
            self.samples.append(row["id"])

    def sample_values(self):
        r = self.rng
        ts = datetime(2020, 1, 1) + timedelta(seconds=r.randint(0, 10**8), microseconds=r.randint(0, 999999))
        return (r.randint(-32768, 32767), r.randint(-2**31, 2**31 - 1), r.randint(-2**63, 2**63 - 1),
                Decimal(r.randint(-10**17, 10**17)) / Decimal(10**6), r.choice([1.5, -0.25, 3.125, 1e-3]),
                r.uniform(-1e9, 1e9), r.random() < 0.5, "text " + "é" * r.randint(0, 5), f"v{r.randint(0, 10**9)}",
                r.choice(["ab", "abc", "x"]), date(2000, 1, 1) + timedelta(days=r.randint(0, 20000)),
                dtime(r.randint(0, 23), r.randint(0, 59), r.randint(0, 59), r.randint(0, 999) * 1000),
                ts, ts.replace(tzinfo=timezone.utc), uuid.UUID(int=r.getrandbits(128)),
                json.dumps({"k": r.randint(0, 9), "tags": ["a", "b"][: r.randint(0, 2)]}), bytes(r.getrandbits(8) for _ in range(8)))

    def long_text(self):
        return "".join(f"{self.rng.getrandbits(32):08x}" for _ in range(600))

    def big_txn(self):
        rows = self.all("UPDATE order_items SET unit_price = unit_price WHERE id %% 97 = %s RETURNING *", (self.rng.randrange(97),))
        self.rec("order_items", "u", rows)
        self.conn.commit()
        self.ledger.flush()
        return len(rows)


def bootstrap(args):
    rng = random.Random(args.seed)
    ledger = Ledger(LEDGER)
    with pg(autocommit=False) as conn:
        conn.row_factory = dict_row
        if conn.execute("SELECT count(*) AS n FROM customers").fetchone()["n"]:
            L.info("already bootstrapped")
            return
        people = [(f"{uuid.UUID(int=rng.getrandbits(128))}@example.com", f"Customer {i}", rng.choice(COUNTRIES))
                  for i in range(2000 * args.scale)]
        rows = conn.execute("INSERT INTO customers (email, full_name, country) "
                            "SELECT * FROM unnest(%s::text[], %s::text[], %s::text[]) RETURNING *",
                            [list(c) for c in zip(*people)]).fetchall()
        for r in rows:
            ledger.add("customers", "c", r)
        catalog = [(f"SKU-{i:05d}", f"Product {i}",
                    "".join(f"{rng.getrandbits(32):08x}" for _ in range(600)) if i % 10 == 0 else f"short description {i}",
                    Decimal(rng.randint(100, 50000)) / 100, rng.randint(0, 500)) for i in range(300)]
        rows = conn.execute("INSERT INTO products (sku, name, description, price, stock) "
                            "SELECT * FROM unnest(%s::text[], %s::text[], %s::text[], %s::numeric[], %s::int[]) RETURNING *",
                            [list(c) for c in zip(*catalog)]).fetchall()
        for r in rows:
            ledger.add("products", "c", r)
        conn.execute("INSERT INTO latency_probe DEFAULT VALUES")
        conn.commit()
    ledger.flush()
    w = Workload(rng, ledger)
    for i in range(10000 * args.scale):
        w.new_order()
        oid = w.orders[-1]
        for _ in range(rng.choice([0, 0, 1, 2, 3])):
            if oid in w.active:
                w.advance_order(oid)
        if i % 10 == 0:
            w.type_sample()
        w.conn.commit()
        if i % 200 == 0:
            ledger.flush()
    ledger.flush()
    L.info("bootstrapped %d customers, %d products, %d orders", len(w.customers), len(w.products), len(w.orders))


def probe(stop):
    with pg() as conn:
        while not stop.is_set():
            conn.execute("INSERT INTO latency_probe DEFAULT VALUES")
            stop.wait(1.0)


def run(args):
    rng = random.Random(args.seed)
    ledger = Ledger(LEDGER)
    w = Workload(rng, ledger)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    if args.probe:
        threading.Thread(target=probe, args=(stop,), daemon=True).start()
    start = time.time()
    next_at = start
    next_big = start + args.big_txn_every if args.big_txn_every else None
    done = errors = 0
    while not stop.is_set() and (not args.duration or time.time() - start < args.duration):
        now = time.time()
        bursting = args.burst_every and (now - start) % args.burst_every < args.burst_len
        try:
            w.step()
            done += 1
        except Exception as e:
            errors += 1
            L.warning("op failed, rolled back: %s", str(e).splitlines()[0])
            stop.wait(0.5)
        if next_big and now >= next_big:
            L.info("big transaction touched %d rows", w.big_txn())
            next_big += args.big_txn_every
        next_at = max(next_at + 1 / (args.rate * (args.burst if bursting else 1)), time.time() - 1)
        if next_at > time.time():
            stop.wait(next_at - time.time())
    ledger.flush()
    summary = {"ops": done, "errors": errors, "seconds": round(time.time() - start, 1), "by_type": w.counts}
    L.info("generator done %s", json.dumps(summary))
    print(json.dumps(summary))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("bootstrap")
    b.add_argument("--seed", type=int, default=7)
    b.add_argument("--scale", type=int, default=1)
    r = sub.add_parser("run")
    r.add_argument("--rate", type=float, default=100)
    r.add_argument("--duration", type=float, default=0)
    r.add_argument("--seed", type=int, default=11)
    r.add_argument("--burst", type=float, default=1)
    r.add_argument("--burst-every", type=float, default=0)
    r.add_argument("--burst-len", type=float, default=10)
    r.add_argument("--big-txn-every", type=float, default=0)
    r.add_argument("--probe", action="store_true")
    args = ap.parse_args()
    bootstrap(args) if args.cmd == "bootstrap" else run(args)


if __name__ == "__main__":
    main()
