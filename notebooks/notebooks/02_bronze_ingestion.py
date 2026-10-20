from pyspark.sql import functions as F

BASE = "/Volumes/workspace/bronze/landing/daily"
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
        .trigger(availableNow=True)      # process all new files, then stop
        .toTable(f"workspace.bronze.{name}")
    )
    query.awaitTermination()
    print(f"{name}: done")





for t in TABLES:
    df = spark.table(f"workspace.bronze.{t}")
    print(t, df.count(), "rows,", df.select("source_file").distinct().count(), "files")




for t in TABLES:
    df = spark.table(f"workspace.bronze.{t}")
    print(t, df.count(), "rows,", df.select("source_file").distinct().count(), "files")



for t in TABLES:
    ingest_bronze(t)

REF = f"{BASE}/reference"
ref_files = {
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

for table, fname in ref_files.items():
    (
        spark.read.option("header", "true").csv(f"{REF}/{fname}")
        .withColumn("source_file", F.lit(fname))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"workspace.bronze.{table}")
    )
    print(table, spark.table(f"workspace.bronze.{table}").count())





for t in TABLES + ["products", "sellers", "category_translation"]:
    print(t, spark.table(f"workspace.bronze.{t}").count())

spark.sql("""
    SELECT order_status, COUNT(*) AS n
    FROM workspace.bronze.orders
    GROUP BY order_status ORDER BY n DESC
""").show()

spark.table("workspace.bronze.orders").select(
    "order_id", "customer_id", "order_status", "source_file", "ingestion_timestamp"
).show(5, truncate=False)

spark.table("workspace.bronze.customers").printSchema()
