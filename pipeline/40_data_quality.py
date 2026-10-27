# Databricks notebook source
import uuid
from datetime import datetime
from pyspark.sql import functions as F

BR, SV, GD, OPS = "workspace.bronze", "workspace.silver", "workspace.gold", "workspace.ops"

DQ_SCHEMA = ("run_id string, checked_at timestamp, layer string, table_name string, "
             "check_name string, rule string, expected string, actual string, "
             "status string, severity string")


def q(sql):
    """Run a query and return its single value."""
    return spark.sql(sql).first()[0]


def record(layer, table, name, rule, expected, actual, passed, severity):
    RESULTS.append((RUN_ID, datetime.now(), layer, table, name, rule,
                    str(expected), str(actual), "PASS" if passed else "FAIL", severity))


def check_equal(layer, table, name, expected, actual, severity="critical"):
    record(layer, table, name, "actual == expected", expected, actual, actual == expected, severity)


def check_zero(layer, table, name, actual, severity="critical"):
    record(layer, table, name, "actual == 0", 0, actual, actual == 0, severity)


def check_max_pct(layer, table, name, pct, limit, severity="warn"):
    record(layer, table, name, f"pct <= {limit}", f"<= {limit}", round(pct, 2), pct <= limit, severity)


def check_unique(layer, table, keys):
    k = ", ".join(keys)
    n = q(f"SELECT COUNT(*) - COUNT(DISTINCT {k}) FROM {table}")
    check_zero(layer, table, f"unique_key({k})", n)


def check_no_nulls(layer, table, col):
    n = q(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
    check_zero(layer, table, f"no_nulls({col})", n)


def check_fk(table, col, dim, dim_col):
    n = q(f"SELECT COUNT(*) FROM {table} f LEFT ANTI JOIN {dim} d ON f.{col} = d.{dim_col}")
    check_zero("gold", table, f"fk({col} -> {dim.split('.')[-1]}.{dim_col})", n)


def log_stage(run_id, stage, status, started, message=""):
    """Write one row per pipeline stage to ops.pipeline_run_log."""
    (spark.createDataFrame(
        [(run_id, stage, status, started, datetime.now(), message)],
        "run_id string, stage string, status string, started_at timestamp, "
        "ended_at timestamp, message string")
     .write.mode("append").saveAsTable(f"{OPS}.pipeline_run_log"))

# COMMAND ----------

RUN_ID = str(uuid.uuid4())[:8]
STARTED = datetime.now()
RESULTS = []

# ---------- 1. RECONCILIATION (bronze -> silver -> gold) ----------
check_equal("silver", f"{SV}.orders", "rowcount_vs_bronze_distinct_valid",
    q(f"SELECT COUNT(DISTINCT order_id) FROM {BR}.orders WHERE customer_id IS NOT NULL"),
    q(f"SELECT COUNT(*) FROM {SV}.orders"))

check_equal("silver", f"{SV}.order_items", "rowcount_vs_bronze_distinct_valid",
    q(f"SELECT COUNT(*) FROM (SELECT DISTINCT order_id, order_item_id FROM {BR}.order_items WHERE price IS NOT NULL)"),
    q(f"SELECT COUNT(*) FROM {SV}.order_items"))

check_equal("silver", f"{SV}.order_payments", "rowcount_vs_bronze_distinct_valid",
    q(f"SELECT COUNT(*) FROM (SELECT DISTINCT order_id, payment_sequential FROM {BR}.order_payments WHERE payment_value IS NOT NULL)"),
    q(f"SELECT COUNT(*) FROM {SV}.order_payments"))

check_equal("silver", f"{SV}.customers", "rowcount_vs_bronze_distinct",
    q(f"SELECT COUNT(DISTINCT customer_id) FROM {BR}.customers"),
    q(f"SELECT COUNT(*) FROM {SV}.customers"))

check_equal("gold", f"{GD}.fact_sales", "rowcount_vs_silver_items",
    q(f"SELECT COUNT(*) FROM {SV}.order_items"),
    q(f"SELECT COUNT(*) FROM {GD}.fact_sales"))

check_equal("gold", f"{GD}.fact_sales", "total_price_vs_silver_items",
    q(f"SELECT COALESCE(SUM(price), 0) FROM {SV}.order_items"),
    q(f"SELECT COALESCE(SUM(price), 0) FROM {GD}.fact_sales"))

# Items whose order was quarantined must show up as 'order not found' in the fact table
check_equal("gold", f"{GD}.fact_sales", "orphan_items_flagged_as_not_found",
    q(f"SELECT COUNT(*) FROM {SV}.order_items i LEFT ANTI JOIN {SV}.orders o ON i.order_id = o.order_id"),
    q(f"SELECT COUNT(*) FROM {GD}.fact_sales WHERE NOT order_found"))

# ---------- 2. UNIQUENESS ----------
check_unique("silver", f"{SV}.orders", ["order_id"])
check_unique("silver", f"{SV}.order_items", ["order_id", "order_item_id"])
check_unique("silver", f"{SV}.order_payments", ["order_id", "payment_sequential"])
check_unique("silver", f"{SV}.customers", ["customer_id"])
check_unique("gold", f"{GD}.fact_sales", ["order_id", "order_item_id"])
check_unique("gold", f"{GD}.dim_customer", ["customer_key"])
check_unique("gold", f"{GD}.dim_product", ["product_key"])
check_unique("gold", f"{GD}.dim_seller", ["seller_key"])
check_unique("gold", f"{GD}.dim_date", ["date_key"])

# ---------- 3. COMPLETENESS ----------
check_no_nulls("silver", f"{SV}.orders", "customer_id")
check_no_nulls("silver", f"{SV}.orders", "purchase_ts")
check_no_nulls("silver", f"{SV}.order_items", "price")
check_no_nulls("silver", f"{SV}.order_payments", "payment_value")
for col in ["customer_key", "product_key", "seller_key", "order_date_key"]:
    check_no_nulls("gold", f"{GD}.fact_sales", col)

# ---------- 4. REFERENTIAL INTEGRITY ----------
check_fk(f"{GD}.fact_sales", "customer_key", f"{GD}.dim_customer", "customer_key")
check_fk(f"{GD}.fact_sales", "product_key", f"{GD}.dim_product", "product_key")
check_fk(f"{GD}.fact_sales", "seller_key", f"{GD}.dim_seller", "seller_key")
check_fk(f"{GD}.fact_sales", "order_date_key", f"{GD}.dim_date", "date_key")

check_zero("gold", f"{GD}.fact_sales", "unknown_product_rows",
           q(f"SELECT COUNT(*) FROM {GD}.fact_sales WHERE product_key = -1"), "warn")
check_zero("gold", f"{GD}.fact_sales", "unknown_seller_rows",
           q(f"SELECT COUNT(*) FROM {GD}.fact_sales WHERE seller_key = -1"), "warn")
check_zero("gold", f"{GD}.fact_sales", "found_orders_with_unknown_date",
           q(f"SELECT COUNT(*) FROM {GD}.fact_sales WHERE order_found AND order_date_key = -1"))

# ---------- 5. SCD TYPE 2 RULES ----------
check_zero("gold", f"{GD}.dim_customer", "exactly_one_current_version_per_customer",
    q(f"""SELECT COUNT(*) FROM (
            SELECT customer_unique_id FROM {GD}.dim_customer
            WHERE customer_unique_id <> 'unknown'
            GROUP BY customer_unique_id
            HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1)"""))

check_zero("gold", f"{GD}.dim_customer", "valid_from_before_valid_to",
    q(f"SELECT COUNT(*) FROM {GD}.dim_customer WHERE valid_from >= valid_to"))

# ---------- 6. BUSINESS RULES ----------
check_zero("gold", f"{GD}.fact_sales", "no_non_positive_price",
           q(f"SELECT COUNT(*) FROM {GD}.fact_sales WHERE price <= 0"))
check_zero("gold", f"{GD}.fact_sales", "no_negative_delivery_days",
           q(f"SELECT COUNT(*) FROM {GD}.fact_sales WHERE delivery_days < 0"), "warn")

for t in ["orders", "order_items", "order_payments", "customers"]:
    quarantined = q(f"SELECT COUNT(*) FROM {SV}.quarantine WHERE source_table = '{t}'")
    bronze_rows = q(f"SELECT COUNT(*) FROM {BR}.{t}")
    check_max_pct("silver", f"{SV}.quarantine", f"quarantine_rate_{t}_pct",
                  100.0 * quarantined / max(bronze_rows, 1), 2.0)

# ---------- 7. BRONZE HEALTH AND FRESHNESS ----------
for t in ["orders", "order_items", "order_payments", "customers"]:
    df = spark.table(f"{BR}.{t}")
    if "_rescued_data" in df.columns:
        check_zero("bronze", f"{BR}.{t}", "no_rescued_data",
                   df.filter("_rescued_data IS NOT NULL").count(), "warn")

bronze_max = (spark.table(f"{BR}.orders")
    .select(F.max(F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1)))).first()[0])
silver_max = spark.table(f"{SV}.orders").select(F.max("file_date")).first()[0]
check_equal("silver", f"{SV}.orders", "silver_caught_up_with_bronze_max_file_date", bronze_max, silver_max)

print(f"run {RUN_ID}: {len(RESULTS)} checks executed")

# COMMAND ----------

results_df = spark.createDataFrame(RESULTS, DQ_SCHEMA)
results_df.write.mode("append").saveAsTable(f"{OPS}.dq_log")

total = len(RESULTS)
passed = sum(1 for r in RESULTS if r[8] == "PASS")
crit_fail = [r for r in RESULTS if r[8] == "FAIL" and r[9] == "critical"]
warn_fail = [r for r in RESULTS if r[8] == "FAIL" and r[9] == "warn"]
print(f"{passed}/{total} passed | {len(crit_fail)} critical failures | {len(warn_fail)} warnings")

(results_df.filter("status = 'FAIL'")
    .select("severity", "table_name", "check_name", "expected", "actual")
    .show(truncate=False))

if crit_fail:
    msg = "; ".join(f"{r[3]} :: {r[4]} (expected {r[6]}, got {r[7]})" for r in crit_fail)
    log_stage(RUN_ID, "data_quality", "FAILED", STARTED, msg[:1000])
    raise Exception(f"Data quality failed with {len(crit_fail)} critical check(s): {msg}")

log_stage(RUN_ID, "data_quality", "SUCCESS", STARTED,
          f"{passed}/{total} checks passed, {len(warn_fail)} warnings")
print("Data quality passed. Pipeline can continue.")