import os
import numpy as np
import pandas as pd

RAW = "/Volumes/workspace/bronze/landing/raw_olist"
OUT = "/Volumes/workspace/bronze/landing/daily"
START, END = "2018-01-01", "2018-03-31"
end_ts = pd.Timestamp(END)
rng = np.random.default_rng(42)

# start clean so the script can be rerun safely
dbutils.fs.rm(OUT, True)
os.makedirs(OUT, exist_ok=True)

# ---------- ORDERS ----------
orders = pd.read_csv(f"{RAW}/olist_orders_dataset.csv")
orders["purchase_date"] = pd.to_datetime(orders["order_purchase_timestamp"]).dt.normalize()
orders = orders[orders["purchase_date"].between(START, END)].copy()
orders["file_date"] = orders["purchase_date"]

# 1) late-arriving orders: file lands 1-5 days after purchase
late_idx = orders.sample(frac=0.03, random_state=1).index
orders.loc[late_idx, "file_date"] = (
    orders.loc[late_idx, "purchase_date"]
    + pd.to_timedelta(rng.integers(1, 6, len(late_idx)), unit="D")
).clip(upper=end_ts)

# 2) status updates: an early 'processing' row, then the final row arrives later
candidates = orders.drop(index=late_idx)
candidates = candidates[candidates["purchase_date"] < end_ts]
upd_idx = candidates.sample(frac=0.05, random_state=2).index

early = orders.loc[upd_idx].copy()
early["order_status"] = "processing"
early["order_delivered_carrier_date"] = np.nan
early["order_delivered_customer_date"] = np.nan
early["file_date"] = early["purchase_date"]

orders.loc[upd_idx, "file_date"] = (
    orders.loc[upd_idx, "purchase_date"]
    + pd.to_timedelta(rng.integers(2, 8, len(upd_idx)), unit="D")
).clip(upper=end_ts)

order_dates = orders[["order_id", "file_date", "purchase_date"]]

orders_out = pd.concat([orders, early], ignore_index=True)
orders_out.loc[orders_out.sample(frac=0.005, random_state=3).index, "customer_id"] = np.nan

# ---------- ORDER ITEMS ----------
items = pd.read_csv(f"{RAW}/olist_order_items_dataset.csv")
items = items.merge(order_dates[["order_id", "file_date"]], on="order_id")
items.loc[items.sample(frac=0.01, random_state=4).index, "price"] = np.nan

# ---------- PAYMENTS ----------
pay = pd.read_csv(f"{RAW}/olist_order_payments_dataset.csv")
pay = pay.merge(order_dates[["order_id", "file_date"]], on="order_id")
pay.loc[pay.sample(frac=0.01, random_state=5).index, "payment_value"] = np.nan

# ---------- CUSTOMERS (with address changes) ----------
cust_raw = pd.read_csv(f"{RAW}/olist_customers_dataset.csv")
cust = orders[["customer_id", "file_date", "purchase_date"]].merge(cust_raw, on="customer_id")
cust = cust.sort_values(["customer_unique_id", "purchase_date"])
cust["order_rank"] = cust.groupby("customer_unique_id").cumcount()

repeat_ids = cust.loc[cust["order_rank"] >= 1, "customer_unique_id"].unique()
movers = set(rng.choice(repeat_ids, size=int(len(repeat_ids) * 0.3), replace=False))
mask = cust["customer_unique_id"].isin(movers) & (cust["order_rank"] >= 1)

new_addr = cust_raw.sample(mask.sum(), replace=True, random_state=6)
cust.loc[mask, "customer_city"] = new_addr["customer_city"].values
cust.loc[mask, "customer_state"] = new_addr["customer_state"].values
cust.loc[mask, "customer_zip_code_prefix"] = new_addr["customer_zip_code_prefix"].values
cust = cust.drop(columns=["order_rank"])

# ---------- WRITE DAILY FILES (with duplicates) ----------
def write_daily(df, name, dup_frac=0.02):
    folder = f"{OUT}/{name}"
    os.makedirs(folder, exist_ok=True)
    total_dups = 0
    for d, g in df.groupby("file_date"):
        dups = g.sample(frac=dup_frac, random_state=7)
        total_dups += len(dups)
        out = pd.concat([g, dups]).drop(columns=["file_date", "purchase_date"], errors="ignore")
        out = out.sample(frac=1, random_state=9)  # shuffle rows
        out.to_csv(f"{folder}/{name}_{d:%Y-%m-%d}.csv", index=False)
    print(f"{name}: {df['file_date'].nunique()} files, {len(df)} rows, {total_dups} duplicates injected")

write_daily(orders_out, "orders")
write_daily(items, "order_items")
write_daily(pay, "order_payments")
write_daily(cust, "customers")

# ---------- REFERENCE TABLES (static copies) ----------
import shutil
ref = f"{OUT}/reference"
os.makedirs(ref, exist_ok=True)
for f in ["olist_products_dataset.csv", "olist_sellers_dataset.csv",
          "product_category_name_translation.csv"]:
    shutil.copy(f"{RAW}/{f}", f"{ref}/{f}")
print("reference files copied")


# ---------- Verify the result ----------
for name in ["orders", "order_items", "order_payments", "customers"]:
    files = dbutils.fs.ls(f"{OUT}/{name}")
    df = spark.read.csv(f"{OUT}/{name}", header=True)
    print(f"{name}: {len(files)} files, {df.count()} rows")

o = spark.read.csv(f"{OUT}/orders", header=True)
print("distinct order_ids:", o.select("order_id").distinct().count())
print("null customer_id rows:", o.filter("customer_id IS NULL").count())
o.groupBy("order_status").count().orderBy("count", ascending=False).show()
