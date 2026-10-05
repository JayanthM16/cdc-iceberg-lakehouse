# Real-Time CDC Pipeline into an Apache Iceberg Lakehouse

PostgreSQL → **Debezium** → **Kafka** → **Spark Structured Streaming** → **Apache Iceberg** (bronze + silver), with
idempotent upserts, soft deletes, schema evolution, time travel, and automated source-vs-lake reconciliation.
Everything runs locally with Docker Compose.

```
 ┌────────────┐  WAL   ┌──────────┐  JSON   ┌───────┐  micro-batch  ┌──────────────────────────────┐
 │ PostgreSQL │ ─────▶ │ Debezium │ ──────▶ │ Kafka │ ────────────▶ │ Spark Structured Streaming   │
 │  (orders)  │        │ (Connect)│         │ topic │               │  ├─ bronze: raw events (append)
 └────────────┘        └──────────┘         └───────┘               │  └─ silver: MERGE INTO (upsert)
        ▲                                                           └───────────────┬──────────────┘
        │                          reconcile (counts, sums, row diff)               ▼
        └────────────────────────────────────────────────────────── Apache Iceberg tables (/warehouse)
```

## Quick start

Requirements: Docker Desktop (≈6 GB RAM free). First run downloads images and Spark packages (a few minutes).

```bash
make up            # Postgres (logical WAL), Kafka (KRaft), Debezium Connect
make connector     # register the Debezium Postgres connector (initial snapshot + streaming)
make stream        # start the Spark streaming job (bronze + silver)
RATE=100 DURATION=300 make generate   # synthetic inserts/updates/deletes (~100 ops/s for 5 min)
make verify        # reconcile Postgres vs Iceberg  -> prints PASS / FAIL
make queries       # analytics, snapshots, time travel, file layout
make evolve        # schema evolution demo (adds a column end-to-end, no downtime)
make clean         # tear everything down incl. volumes
```

Watch the stream: `docker compose logs -f spark-stream` (one line per micro-batch).
Inspect Kafka Connect: `curl localhost:8083/connectors/orders-connector/status`.

## Design decisions (good interview material)

| Decision | Why |
|---|---|
| Order by **WAL LSN**, not arrival time | Kafka only orders within a partition; LSN is the true commit order in the source DB. |
| **Idempotent MERGE** guarded by `s.lsn > t.src_lsn` | Replays, retries, and out-of-order events can't move a row backwards → effectively-once with checkpointed offsets. |
| **Soft deletes** (`is_deleted`) | A late older UPDATE can't resurrect a hard-deleted row. Hard-purge later with a retention job. |
| **Dedup per key inside each micro-batch** | MERGE requires ≤1 source row per target row; also reduces write amplification. |
| **Bronze keeps raw JSON** | Full audit/replay; silver can be rebuilt after any bug or schema change. |
| **Iceberg v2, merge-on-read, `bucket(8, order_id)` hidden partitioning** | Cheap row-level upserts; partition layout can evolve without rewriting data. |
| **`REPLICA IDENTITY FULL`** | Delete events carry the whole row, not just the key. |
| **Schema evolution** | MERGE column list = columns present in both event schema and silver; widen silver first (`make evolve`). |

## Metrics to capture for your resume (run, then fill in real numbers)

* **Throughput:** `RATE=500 DURATION=300 make generate`, then check batch sizes/times in the Spark logs.
* **End-to-end latency:** query 7 in `sql/queries.sql` (p50/p95 from source commit to Iceberg ingest).
* **Correctness:** `make verify` → `RESULT: PASS`, with the row count and `sum(amount)` shown.
* **Small-file check:** query 5 in `sql/queries.sql`; run `rewrite_data_files` and compare file counts/avg size before/after.
* **Recovery:** `docker compose stop spark-stream`, generate traffic, restart it, run `make verify` (still PASS).

## Limitations / next steps

* Hadoop catalog on a local volume for simplicity. Production: REST/Glue/Nessie catalog + S3 (see note in `spark/common.py`).
* Bronze is append-only without dedup, so a retried micro-batch can duplicate raw rows (silver stays correct).
* Single Kafka broker, single Spark container. Add partitions/executors to scale out.
* Not included: Avro + Schema Registry, dead-letter topic for malformed events, Prometheus/Grafana monitoring. Each is a good extension.
