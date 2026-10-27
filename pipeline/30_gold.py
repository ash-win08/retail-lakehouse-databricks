# Databricks notebook source
from pyspark.sql import functions as F, Window

SV = "workspace.silver"
GD = "workspace.gold"


def save(df, name):
    (df.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{GD}.{name}"))
    n = spark.table(f"{GD}.{name}").count()
    print(f"{name}: {n} rows")

# COMMAND ----------

dates = spark.sql("""
    SELECT explode(sequence(to_date('2017-01-01'), to_date('2019-12-31'), interval 1 day)) AS full_date
""")

dim_date = dates.select(
    F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
    "full_date",
    F.year("full_date").alias("year"),
    F.quarter("full_date").alias("quarter"),
    F.month("full_date").alias("month"),
    F.date_format("full_date", "MMMM").alias("month_name"),
    F.dayofmonth("full_date").alias("day"),
    F.date_format("full_date", "EEEE").alias("day_name"),
    F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
)

unknown_date = spark.sql("""
    SELECT -1 AS date_key, CAST(NULL AS DATE) AS full_date, CAST(NULL AS INT) AS year,
           CAST(NULL AS INT) AS quarter, CAST(NULL AS INT) AS month,
           'Unknown' AS month_name, CAST(NULL AS INT) AS day,
           'Unknown' AS day_name, CAST(NULL AS BOOLEAN) AS is_weekend
""")

save(dim_date.unionByName(unknown_date), "dim_date")

# COMMAND ----------

p = spark.table(f"{SV}.products")
dim_product = p.select(
    F.xxhash64("product_id").alias("product_key"),   # stable surrogate key
    "product_id",
    F.col("category_name_english").alias("category"),
    F.col("category_name").alias("category_pt"),
    "weight_g", "length_cm", "height_cm", "width_cm", "photos_qty",
)

unknown_product = spark.sql("""
    SELECT CAST(-1 AS BIGINT) AS product_key, 'unknown' AS product_id,
           'unknown' AS category, 'unknown' AS category_pt,
           CAST(NULL AS INT) AS weight_g, CAST(NULL AS INT) AS length_cm,
           CAST(NULL AS INT) AS height_cm, CAST(NULL AS INT) AS width_cm,
           CAST(NULL AS INT) AS photos_qty
""")
save(dim_product.unionByName(unknown_product), "dim_product")


s = spark.table(f"{SV}.sellers")
dim_seller = s.select(
    F.xxhash64("seller_id").alias("seller_key"),
    "seller_id", "zip_code_prefix", "city", "state",
)

unknown_seller = spark.sql("""
    SELECT CAST(-1 AS BIGINT) AS seller_key, 'unknown' AS seller_id,
           '00000' AS zip_code_prefix, 'unknown' AS city, 'unknown' AS state
""")
save(dim_seller.unionByName(unknown_seller), "dim_seller")

# COMMAND ----------

c = spark.table(f"{SV}.customers").select(
    "customer_id", "customer_unique_id", "zip_code_prefix", "city", "state")
o = spark.table(f"{SV}.orders").select("customer_id", "purchase_ts")

# One row per order-level customer record, with the purchase time
x = c.join(o, "customer_id", "inner")

w = Window.partitionBy("customer_unique_id").orderBy("purchase_ts", "customer_id")

x = (
    x.withColumn("addr", F.concat_ws("|", "zip_code_prefix", "city", "state"))
     .withColumn("prev_addr", F.lag("addr").over(w))
     # a new version starts when it's the first row or the address differs from the previous one
     .withColumn("is_change", F.col("prev_addr").isNull() | (F.col("prev_addr") != F.col("addr")))
     # carry the start time of the current version down to the following rows
     .withColumn(
         "version_start",
         F.last(F.when(F.col("is_change"), F.col("purchase_ts")), ignorenulls=True)
          .over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
     .withColumn("customer_key",
                 F.xxhash64("customer_unique_id", F.col("version_start").cast("string")))
)

# Bridge: which dimension version was each order placed under?
bridge = x.select("customer_id", "customer_key")

# Dimension: one row per version
versions = (
    x.filter("is_change")
     .select("customer_key", "customer_unique_id", "zip_code_prefix", "city", "state",
             F.col("purchase_ts").alias("valid_from"))
     .dropDuplicates(["customer_key"])        # guards against exact-timestamp ties
)

w2 = Window.partitionBy("customer_unique_id").orderBy("valid_from")
dim_customer = (
    versions
    .withColumn("valid_to", F.lead("valid_from").over(w2))
    .withColumn("is_current", F.col("valid_to").isNull())
    .withColumn("valid_to",
                F.coalesce("valid_to", F.lit("9999-12-31 00:00:00").cast("timestamp")))
)

unknown_customer = spark.sql("""
    SELECT CAST(-1 AS BIGINT) AS customer_key, 'unknown' AS customer_unique_id,
           '00000' AS zip_code_prefix, 'unknown' AS city, 'unknown' AS state,
           TIMESTAMP'1900-01-01 00:00:00' AS valid_from,
           TIMESTAMP'9999-12-31 00:00:00' AS valid_to, true AS is_current
""")
save(dim_customer.unionByName(unknown_customer), "dim_customer")

# COMMAND ----------

items  = spark.table(f"{SV}.order_items")
orders = spark.table(f"{SV}.orders").select(
    "order_id", "customer_id", "order_status", "purchase_ts",
    "customer_delivered_ts", "estimated_delivery_ts")
pay    = spark.table(f"{SV}.order_payments")

# One "main" payment per order: the largest one
wp = Window.partitionBy("order_id").orderBy(F.col("payment_value").desc(), "payment_sequential")
main_pay = (pay.withColumn("rn", F.row_number().over(wp)).filter("rn = 1")
               .select("order_id", "payment_type", "payment_installments"))

dp = spark.table(f"{GD}.dim_product").select("product_id", "product_key")
ds = spark.table(f"{GD}.dim_seller").select("seller_id", "seller_key")

fact_sales = (
    items
    .join(orders, "order_id", "left")      # left joins: nothing is dropped silently
    .join(bridge, "customer_id", "left")
    .join(main_pay, "order_id", "left")
    .join(dp, "product_id", "left")
    .join(ds, "seller_id", "left")
    .select(
        "order_id",
        "order_item_id",
        F.coalesce("customer_key", F.lit(-1)).alias("customer_key"),
        F.coalesce("product_key", F.lit(-1)).alias("product_key"),
        F.coalesce("seller_key", F.lit(-1)).alias("seller_key"),
        F.coalesce(F.date_format("purchase_ts", "yyyyMMdd").cast("int"), F.lit(-1)).alias("order_date_key"),
        F.coalesce(F.date_format("customer_delivered_ts", "yyyyMMdd").cast("int"), F.lit(-1)).alias("delivered_date_key"),
        F.coalesce("order_status", F.lit("unknown")).alias("order_status"),
        F.coalesce("payment_type", F.lit("unknown")).alias("payment_type"),
        "payment_installments",
        "price",
        "freight_value",
        (F.col("price") + F.col("freight_value")).alias("item_total"),
        F.datediff(F.to_date("customer_delivered_ts"), F.to_date("purchase_ts")).alias("delivery_days"),
        (F.col("customer_delivered_ts") > F.col("estimated_delivery_ts")).alias("is_late_delivery"),
        F.col("purchase_ts").isNotNull().alias("order_found"),
    )
)

save(fact_sales, "fact_sales")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.agg_daily_revenue AS
SELECT d.full_date AS order_date,
       COUNT(DISTINCT f.order_id) AS orders,
       COUNT(*) AS items,
       SUM(f.price) AS revenue,
       SUM(f.freight_value) AS freight
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_date d ON f.order_date_key = d.date_key
WHERE f.order_found AND f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.full_date
ORDER BY d.full_date
""")

spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.agg_category_revenue AS
SELECT p.category,
       COUNT(*) AS items,
       SUM(f.price) AS revenue
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_product p ON f.product_key = p.product_key
WHERE f.order_found AND f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY p.category
ORDER BY revenue DESC
""")

spark.sql("""
CREATE OR REPLACE TABLE workspace.gold.agg_customer_orders AS
SELECT c.customer_unique_id,
       COUNT(DISTINCT f.order_id) AS orders,
       SUM(f.price) AS revenue
FROM workspace.gold.fact_sales f
JOIN workspace.gold.dim_customer c ON f.customer_key = c.customer_key
WHERE f.order_found AND f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.customer_unique_id
""")
print("aggregates built")