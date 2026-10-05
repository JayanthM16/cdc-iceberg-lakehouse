"""Synthetic OLTP traffic: ~50% inserts, ~40% updates, ~10% deletes on public.orders."""
import os
import random
import time

import psycopg2

RATE = float(os.getenv("RATE", "50"))          # operations per second
DURATION = int(os.getenv("DURATION", "120"))   # seconds
STATUSES = ["created", "paid", "shipped", "delivered", "cancelled"]

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"), port=int(os.getenv("PGPORT", "5432")),
    dbname="shop", user="app", password="app",
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("SELECT COALESCE(MAX(order_id), 0) FROM public.orders")
next_id = cur.fetchone()[0] + 1
cur.execute("SELECT order_id FROM public.orders")
live = {r[0] for r in cur.fetchall()}

counts = {"insert": 0, "update": 0, "delete": 0}
start, interval = time.time(), 1.0 / RATE
while time.time() - start < DURATION:
    roll = random.random()
    if roll < 0.5 or not live:
        cur.execute("INSERT INTO public.orders VALUES (%s,%s,%s,%s)",
                    (next_id, random.randint(1, 5000), "created", round(random.uniform(5, 500), 2)))
        live.add(next_id)
        next_id += 1
        counts["insert"] += 1
    elif roll < 0.9:
        oid = random.choice(tuple(live))
        cur.execute("UPDATE public.orders SET status=%s, amount=%s WHERE order_id=%s",
                    (random.choice(STATUSES), round(random.uniform(5, 500), 2), oid))
        counts["update"] += 1
    else:
        oid = random.choice(tuple(live))
        cur.execute("DELETE FROM public.orders WHERE order_id=%s", (oid,))
        live.discard(oid)
        counts["delete"] += 1
    time.sleep(interval)

total = sum(counts.values())
print(f"done: {total} ops in {time.time()-start:.0f}s ({total/(time.time()-start):.1f} ops/s) {counts}")
