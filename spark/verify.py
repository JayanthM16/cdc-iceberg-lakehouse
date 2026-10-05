"""Reconciliation: PostgreSQL source of truth vs Iceberg silver. Exits 1 on any mismatch."""
import sys

from pyspark.sql import functions as F

from common import get_spark

spark = get_spark("verify-reconciliation")
spark.sparkContext.setLogLevel("ERROR")

pg = (spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://postgres:5432/shop")
      .option("dbtable", "public.orders")
      .option("user", "app").option("password", "app")
      .option("driver", "org.postgresql.Driver").load()
      .select("order_id", "customer_id", "status",
              F.round(F.col("amount").cast("double"), 2).alias("amount")))

lake = (spark.table("lake.silver.orders").where("NOT is_deleted")
        .select("order_id", "customer_id", "status", F.round("amount", 2).alias("amount")))

pg_n, lake_n = pg.count(), lake.count()
pg_sum = pg.agg(F.round(F.sum("amount"), 2)).first()[0]
lake_sum = lake.agg(F.round(F.sum("amount"), 2)).first()[0]

diff = (pg.alias("p").join(lake.alias("l"), "order_id", "full_outer")
        .where("p.customer_id IS NULL OR l.customer_id IS NULL "
               "OR p.customer_id <> l.customer_id OR p.status <> l.status OR p.amount <> l.amount"))
mismatches = diff.count()

print("\n========== RECONCILIATION ==========")
print(f"rows      postgres={pg_n:<8} iceberg={lake_n:<8} {'OK' if pg_n == lake_n else 'MISMATCH'}")
print(f"sum(amt)  postgres={pg_sum:<8} iceberg={lake_sum:<8} {'OK' if pg_sum == lake_sum else 'MISMATCH'}")
print(f"row-level mismatches: {mismatches}")
if mismatches:
    diff.show(20, truncate=False)
print("RESULT:", "PASS" if (pg_n == lake_n and pg_sum == lake_sum and mismatches == 0) else "FAIL")
print("====================================\n")
sys.exit(0 if (pg_n == lake_n and pg_sum == lake_sum and mismatches == 0) else 1)
