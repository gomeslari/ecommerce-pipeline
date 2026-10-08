"""PySpark ETL job: cleans and validates raw e-commerce data, then computes
sales metrics by category and by state.

Run locally:
    python jobs/process_orders.py

Reads from:  data/raw/{customers,products,orders,order_items}.csv
Writes to:   data/processed/{sales_by_category,sales_by_state,data_quality_report}/
"""
import os
import sys

from pyspark.sql import SparkSession, functions as F

# Pin Spark's Python workers to the same interpreter running this driver script.
# Without this, environments like Google Colab can spawn workers on a different
# system Python than the one running the driver, causing a PYTHON_VERSION_MISMATCH.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")


def get_spark():
    return (
        SparkSession.builder.appName("ecommerce-sales-pipeline")
        .master("local[*]")
        .getOrCreate()
    )


def load_raw_tables(spark):
    customers = spark.read.csv(os.path.join(RAW_DIR, "customers.csv"), header=True, inferSchema=True)
    products = spark.read.csv(os.path.join(RAW_DIR, "products.csv"), header=True, inferSchema=True)
    orders = spark.read.csv(os.path.join(RAW_DIR, "orders.csv"), header=True, inferSchema=True)
    order_items = spark.read.csv(os.path.join(RAW_DIR, "order_items.csv"), header=True, inferSchema=True)
    return customers, products, orders, order_items


def clean_products(products):
    """Normalizes category text: trims whitespace and title-cases values."""
    return products.withColumn("category", F.initcap(F.trim(F.col("category"))))


def validate_order_items(order_items, orders, products):
    """Applies data-quality rules and splits order_items into valid/rejected sets.

    Rules:
      - order_id must exist in orders (referential integrity)
      - product_id must exist in products (referential integrity)
      - quantity must be > 0
      - unit_price must be >= 0
      - exact duplicate rows are dropped, keeping the first occurrence
    """
    deduped = order_items.dropDuplicates(
        ["order_id", "product_id", "quantity", "unit_price", "freight_value"]
    )
    n_dupes_removed = order_items.count() - deduped.count()

    order_id_set = orders.select("order_id").distinct()
    product_id_set = products.select("product_id").distinct()

    with_flags = (
        deduped
        .join(order_id_set.withColumn("_order_ok", F.lit(True)), on="order_id", how="left")
        .join(product_id_set.withColumn("_product_ok", F.lit(True)), on="product_id", how="left")
        .withColumn("_order_ok", F.coalesce(F.col("_order_ok"), F.lit(False)))
        .withColumn("_product_ok", F.coalesce(F.col("_product_ok"), F.lit(False)))
        .withColumn("_qty_ok", F.col("quantity") > 0)
        .withColumn("_price_ok", F.col("unit_price") >= 0)
    )

    is_valid = F.col("_order_ok") & F.col("_product_ok") & F.col("_qty_ok") & F.col("_price_ok")

    valid_items = with_flags.filter(is_valid).drop("_order_ok", "_product_ok", "_qty_ok", "_price_ok")
    rejected_items = with_flags.filter(~is_valid)

    quality_report = {
        "total_rows_ingested": order_items.count(),
        "duplicate_rows_removed": n_dupes_removed,
        "rows_with_invalid_order_id": rejected_items.filter(~F.col("_order_ok")).count(),
        "rows_with_invalid_product_id": rejected_items.filter(~F.col("_product_ok")).count(),
        "rows_with_invalid_quantity": rejected_items.filter(~F.col("_qty_ok")).count(),
        "rows_with_invalid_price": rejected_items.filter(~F.col("_price_ok")).count(),
        "valid_rows_output": valid_items.count(),
    }
    return valid_items, quality_report


def build_sales_metrics(valid_items, orders, products, customers):
    enriched = (
        valid_items
        .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
        .join(orders, on="order_id", how="inner")
        .join(products.select("product_id", "category"), on="product_id", how="inner")
        .join(customers.select("customer_id", "state"), on="customer_id", how="inner")
        .filter(F.col("status") != "canceled")
    )

    sales_by_category = (
        enriched.groupBy("category")
        .agg(
            F.round(F.sum("line_total"), 2).alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("order_id").alias("total_orders"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    sales_by_state = (
        enriched.groupBy("state")
        .agg(
            F.round(F.sum("line_total"), 2).alias("total_revenue"),
            F.countDistinct("customer_id").alias("distinct_customers"),
            F.countDistinct("order_id").alias("total_orders"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    return sales_by_category, sales_by_state


def write_single_csv(df, out_path):
    """Writes a small aggregated DataFrame as a single readable CSV file."""
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(out_path)
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    customers, products, orders, order_items = load_raw_tables(spark)
    products = clean_products(products)

    valid_items, quality_report = validate_order_items(order_items, orders, products)

    print("\n=== DATA QUALITY REPORT ===")
    for key, value in quality_report.items():
        print(f"{key}: {value}")
    print("============================\n")

    sales_by_category, sales_by_state = build_sales_metrics(valid_items, orders, products, customers)

    print("=== SALES BY CATEGORY ===")
    sales_by_category.show(truncate=False)

    print("=== SALES BY STATE ===")
    sales_by_state.show(truncate=False)

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    write_single_csv(sales_by_category, os.path.join(PROCESSED_DIR, "sales_by_category"))
    write_single_csv(sales_by_state, os.path.join(PROCESSED_DIR, "sales_by_state"))

    quality_df = spark.createDataFrame([quality_report])
    write_single_csv(quality_df, os.path.join(PROCESSED_DIR, "data_quality_report"))

    spark.stop()


if __name__ == "__main__":
    main()
