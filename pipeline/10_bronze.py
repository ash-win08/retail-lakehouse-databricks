# Databricks notebook source
from pyspark.sql import functions as F

BASE = "/Volumes/workspace/bronze/landing/incoming"
REF  = "/Volumes/workspace/bronze/landing/daily/reference"
CHK  = "/Volumes/workspace/bronze/landing/_checkpoints"
TABLES = ["orders", "order_items", "order_payments", "customers"]


def ingest_bronze(name, file_glob=None):
    """Incrementally load new daily CSV files for one table into a bronze Delta table."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{CHK}/{name}/schema")
    )
    if file_glob:
        reader = reader.option("pathGlobFilter", file_glob)

    df = (
        reader.load(f"{BASE}/{name}")
        .select(
            "*",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )

    query = (
        df.writeStream
        .option("checkpointLocation", f"{CHK}/{name}/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"workspace.bronze.{name}")
    )
    query.awaitTermination()
    rows = sum((p.get("numInputRows") or 0) for p in query.recentProgress)
    print(f"{name}: done, {rows} new rows ingested")

# COMMAND ----------

for t in TABLES:
    ingest_bronze(t)

ref_files = {
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}
for table, fname in ref_files.items():
    (spark.read.option("header", "true").csv(f"{REF}/{fname}")
        .withColumn("source_file", F.lit(fname))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"workspace.bronze.{table}"))
    print(table, "loaded")

# COMMAND ----------

for t in TABLES:
    df = spark.table(f"workspace.bronze.{t}")
    n_files = df.select("source_file").distinct().count()
    print(f"{t}: {df.count()} rows from {n_files} files")