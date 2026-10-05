"""
Kafka (Debezium CDC events) -> Apache Iceberg, using Spark Structured Streaming.

Layers
  lake.bronze.orders_cdc : append-only raw change events (audit / replay)
  lake.silver.orders     : current state per order_id (upserted with MERGE INTO)

Correctness model
  * Ordering key is the PostgreSQL WAL position (source.lsn), not arrival time.
  * Within a micro-batch only the newest event per key is kept.
  * MERGE applies an event only when its lsn > the lsn already stored  ->  idempotent,
    so replays, retries and out-of-order / late events never move a row backwards.
    (Effectively-once: checkpointed offsets + idempotent sink.)
  * Deletes are soft (is_deleted = true) so a late older UPDATE can't resurrect a row.
  * Schema evolution: bronze keeps the raw JSON; the MERGE column list is built from the
    columns that exist in BOTH the event schema and the silver table.
"""
import os

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

from common import get_spark

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_PATTERN = os.getenv("TOPIC_PATTERN", r"shop\.public\.orders")
CHECKPOINT = os.getenv("CHECKPOINT", "/checkpoints/orders")
MAX_OFFSETS = os.getenv("MAX_OFFSETS_PER_TRIGGER", "5000")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "10 seconds")

BRONZE = "lake.bronze.orders_cdc"
SILVER = "lake.silver.orders"

# Columns we know how to merge. `channel` is optional: it only flows to silver once the column exists there.
DATA_COLS = ["customer_id", "status", "amount", "channel"]

ROW = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("status", StringType()),
    StructField("amount", DoubleType()),
    StructField("channel", StringType()),
])
ENVELOPE = StructType([
    StructField("before", ROW),
    StructField("after", ROW),
    StructField("source", StructType([
        StructField("ts_ms", LongType()),
        StructField("lsn", LongType()),
    ])),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
])

spark = get_spark("cdc-orders-to-iceberg")
spark.sparkContext.setLogLevel("WARN")


def create_tables():
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE} (
            order_id        INT,
            op              STRING,
            lsn             BIGINT,
            source_ts_ms    BIGINT,
            kafka_partition INT,
            kafka_offset    BIGINT,
            raw             STRING,
            ingested_at     TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(ingested_at))
        TBLPROPERTIES ('format-version'='2')
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER} (
            order_id    INT,
            customer_id INT,
            status      STRING,
            amount      DOUBLE,
            is_deleted  BOOLEAN,
            src_lsn     BIGINT,
            cdc_ts_ms   BIGINT
        ) USING iceberg
        PARTITIONED BY (bucket(8, order_id))
        TBLPROPERTIES (
            'format-version'='2',
            'write.merge.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.delete.mode'='merge-on-read'
        )
    """)


def parse(df):
    parsed = df.select(
        "kafka_partition", "kafka_offset", "raw",
        F.from_json("raw", ENVELOPE).alias("e"),
    )
    cols = [
        F.coalesce(F.col("e.after.order_id"), F.col("e.before.order_id")).alias("order_id"),
        F.col("e.op").alias("op"),
        F.col("e.source.lsn").alias("lsn"),
        F.coalesce(F.col("e.source.ts_ms"), F.col("e.ts_ms")).alias("source_ts_ms"),
        "kafka_partition", "kafka_offset", "raw",
    ]
    # deletes have after = null -> fall back to the before image (REPLICA IDENTITY FULL)
    for c in DATA_COLS:
        cols.append(F.coalesce(F.col(f"e.after.{c}"), F.col(f"e.before.{c}")).alias(c))
    return (parsed.select(*cols)
            .where(F.col("op").isin("c", "u", "d", "r"))
            .where(F.col("order_id").isNotNull() & F.col("lsn").isNotNull()))


def process_batch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return
    events = parse(batch_df).cache()

    # 1) bronze: raw, append-only
    (events.select("order_id", "op", "lsn", "source_ts_ms", "kafka_partition",
                   "kafka_offset", "raw")
           .withColumn("ingested_at", F.current_timestamp())
           .writeTo(BRONZE).append())

    # 2) silver: newest event per key, then idempotent MERGE
    w = Window.partitionBy("order_id").orderBy(F.col("lsn").desc(), F.col("source_ts_ms").desc())
    latest = (events.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn"))
    latest.createOrReplaceTempView("latest")

    target_cols = set(spark.table(SILVER).columns)
    cols = [c for c in DATA_COLS if c in target_cols]          # schema-evolution aware
    set_clause = ", ".join([f"{c} = s.{c}" for c in cols] +
                           ["is_deleted = (s.op = 'd')", "src_lsn = s.lsn", "cdc_ts_ms = s.source_ts_ms"])
    insert_cols = ", ".join(["order_id"] + cols + ["is_deleted", "src_lsn", "cdc_ts_ms"])
    insert_vals = ", ".join(["s.order_id"] + [f"s.{c}" for c in cols] +
                            ["(s.op = 'd')", "s.lsn", "s.source_ts_ms"])

    spark.sql(f"""
        MERGE INTO {SILVER} t
        USING latest s
        ON t.order_id = s.order_id
        WHEN MATCHED AND s.lsn > t.src_lsn THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
    n = events.count()
    events.unpersist()
    print(f"[batch {batch_id}] applied {n} change events", flush=True)


def main():
    create_tables()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)     # tolerates the topic not existing yet
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
        .where(F.col("value").isNotNull())
        .select(
            F.col("value").cast("string").alias("raw"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
    )
    (raw.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER)
        .start()
        .awaitTermination())


if __name__ == "__main__":
    main()
