# Databricks notebook source
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

BR  = "workspace.bronze"
SV  = "workspace.silver"
OPS = "workspace.ops"
CHK = "/Volumes/workspace/bronze/landing/_checkpoints_silver"


def tcast(col, dtype):
    """Safe cast: bad values become NULL instead of crashing the job."""
    return F.expr(f"try_cast(trim({col}) AS {dtype})")


def tts(col):
    """Safe timestamp parse."""
    return F.expr(f"try_to_timestamp(trim({col}), 'yyyy-MM-dd HH:mm:ss')")


def file_date():
    """Arrival date, taken from the filename (e.g. orders_2018-01-05.csv)."""
    return F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1))

# COMMAND ----------

VALID_STATUS = ["created", "approved", "invoiced", "processing",
                "shipped", "delivered", "unavailable", "canceled"]


def t_orders(df):
    out = df.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.lower(F.trim("order_status")).alias("order_status"),
        tts("order_purchase_timestamp").alias("purchase_ts"),
        tts("order_approved_at").alias("approved_ts"),
        tts("order_delivered_carrier_date").alias("carrier_delivered_ts"),
        tts("order_delivered_customer_date").alias("customer_delivered_ts"),
        tts("order_estimated_delivery_date").alias("estimated_delivery_ts"),
        file_date().alias("file_date"),
        "source_file",
    )
    return out.withColumn(
        "dq_reason",
        F.when(F.col("order_id").isNull(), "missing_order_id")
         .when(F.col("customer_id").isNull(), "missing_customer_id")
         .when(F.col("order_status").isNull()
               | ~F.col("order_status").isin(VALID_STATUS), "invalid_status")
         .when(F.col("purchase_ts").isNull(), "invalid_purchase_timestamp"),
    )


def t_items(df):
    out = df.select(
        F.trim("order_id").alias("order_id"),
        tcast("order_item_id", "int").alias("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        tts("shipping_limit_date").alias("shipping_limit_ts"),
        tcast("price", "decimal(10,2)").alias("price"),
        tcast("freight_value", "decimal(10,2)").alias("freight_value"),
        file_date().alias("file_date"),
        "source_file",
    )
    return out.withColumn(
        "dq_reason",
        F.when(F.col("order_id").isNull() | F.col("order_item_id").isNull(), "missing_key")
         .when(F.col("price").isNull(), "missing_price")
         .when(F.col("price") <= 0, "non_positive_price"),
    )


def t_payments(df):
    out = df.select(
        F.trim("order_id").alias("order_id"),
        tcast("payment_sequential", "int").alias("payment_sequential"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        tcast("payment_installments", "int").alias("payment_installments"),
        tcast("payment_value", "decimal(10,2)").alias("payment_value"),
        file_date().alias("file_date"),
        "source_file",
    )
    return out.withColumn(
        "dq_reason",
        F.when(F.col("order_id").isNull() | F.col("payment_sequential").isNull(), "missing_key")
         .when(F.col("payment_value").isNull(), "missing_payment_value")
         .when(F.col("payment_value") < 0, "negative_payment_value"),
    )


def t_customers(df):
    out = df.select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        F.lpad(F.trim("customer_zip_code_prefix"), 5, "0").alias("zip_code_prefix"),
        F.lower(F.trim("customer_city")).alias("city"),
        F.upper(F.trim("customer_state")).alias("state"),
        file_date().alias("file_date"),
        "source_file",
    )
    return out.withColumn(
        "dq_reason",
        F.when(F.col("customer_id").isNull() | F.col("customer_unique_id").isNull(), "missing_key"),
    )

# COMMAND ----------

SPECS = {
    "orders":         (t_orders,    ["order_id"]),
    "order_items":    (t_items,     ["order_id", "order_item_id"]),
    "order_payments": (t_payments,  ["order_id", "payment_sequential"]),
    "customers":      (t_customers, ["customer_id"]),
}


def make_processor(name, transform, keys):
    target = f"{SV}.{name}"

    def process(batch_df, batch_id):
        s = batch_df.sparkSession
        df = transform(batch_df)
        good = df.filter("dq_reason IS NULL").drop("dq_reason")
        bad = df.filter("dq_reason IS NOT NULL")

        # 1) Quarantine bad rows (with the reason and the full record)
        payload_cols = [c for c in bad.columns if c not in ("dq_reason", "source_file")]
        (bad.select(
                F.lit(name).alias("source_table"),
                F.col("dq_reason").alias("reason"),
                F.to_json(F.struct(*payload_cols), {"ignoreNullFields": "false"}).alias("record_json"),
                "source_file",
                F.current_timestamp().alias("quarantined_at"))
            .dropDuplicates(["source_table", "reason", "record_json", "source_file"])
            .write.mode("append").saveAsTable(f"{SV}.quarantine"))

        # 2) Keep only the latest version of each key within this batch
        w = Window.partitionBy(*keys).orderBy(F.col("file_date").desc())
        latest = (good.withColumn("_rn", F.row_number().over(w))
                      .filter("_rn = 1").drop("_rn")
                      .withColumn("silver_updated_at", F.current_timestamp()))

        # 3) MERGE into silver (create the table on the first batch)
        if not s.catalog.tableExists(target):
            latest.limit(0).write.saveAsTable(target)
        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (DeltaTable.forName(s, target).alias("t")
            .merge(latest.alias("s"), cond)
            .whenMatchedUpdateAll(condition="s.file_date >= t.file_date")
            .whenNotMatchedInsertAll()
            .execute())

        # 4) Log the batch
        n_in, n_bad = df.count(), bad.count()
        s.createDataFrame(
            [(name, int(batch_id), n_in, n_in - n_bad, n_bad, datetime.now())],
            "table_name string, batch_id long, rows_in long, rows_valid long, "
            "rows_quarantined long, logged_at timestamp",
        ).write.mode("append").saveAsTable(f"{OPS}.silver_run_log")

    return process

# COMMAND ----------

def run_silver(name):
    transform, keys = SPECS[name]
    (spark.readStream.table(f"{BR}.{name}")
        .writeStream
        .foreachBatch(make_processor(name, transform, keys))
        .option("checkpointLocation", f"{CHK}/{name}")
        .trigger(availableNow=True)
        .start()
        .awaitTermination())
    print(f"{name}: done")


for n in SPECS:
    run_silver(n)

# COMMAND ----------

# products, with the English category name
cat = spark.table(f"{BR}.category_translation").select(
    F.trim("product_category_name").alias("category_name"),
    F.trim("product_category_name_english").alias("category_name_english"))

products = (
    spark.table(f"{BR}.products").select(
        F.trim("product_id").alias("product_id"),
        F.coalesce(F.trim("product_category_name"), F.lit("unknown")).alias("category_name"),
        tcast("product_name_lenght", "int").alias("name_length"),
        tcast("product_description_lenght", "int").alias("description_length"),
        tcast("product_photos_qty", "int").alias("photos_qty"),
        tcast("product_weight_g", "int").alias("weight_g"),
        tcast("product_length_cm", "int").alias("length_cm"),
        tcast("product_height_cm", "int").alias("height_cm"),
        tcast("product_width_cm", "int").alias("width_cm"))
    .dropDuplicates(["product_id"])
    .join(cat, "category_name", "left")
    .withColumn("category_name_english",
                F.coalesce("category_name_english", "category_name"))
)
products.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SV}.products")

sellers = (
    spark.table(f"{BR}.sellers").select(
        F.trim("seller_id").alias("seller_id"),
        F.lpad(F.trim("seller_zip_code_prefix"), 5, "0").alias("zip_code_prefix"),
        F.lower(F.trim("seller_city")).alias("city"),
        F.upper(F.trim("seller_state")).alias("state"))
    .dropDuplicates(["seller_id"])
)
sellers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SV}.sellers")

print("products:", spark.table(f"{SV}.products").count(),
      "| sellers:", spark.table(f"{SV}.sellers").count())