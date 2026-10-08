"""Airflow DAG orchestrating the e-commerce sales pipeline.

Tasks:
  1. generate_data   -> builds the raw synthetic dataset (stand-in for a source extract)
  2. validate_raw    -> fails fast if any raw file is missing/empty before Spark runs
  3. run_spark_job   -> runs jobs/process_orders.py (clean, validate, aggregate)
  4. check_output    -> fails the DAG if the data-quality report shows zero valid rows

This mirrors a realistic batch pattern: ingest -> validate -> transform -> verify output,
with each stage as its own task so failures are isolated and visible in the Airflow UI.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import os
import subprocess
import sys

PROJECT_DIR = os.environ.get("ECOMMERCE_PROJECT_DIR", "/content/ecommerce-pipeline")
RAW_DIR = os.path.join(PROJECT_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_DIR, "data", "processed")

REQUIRED_RAW_FILES = ["customers.csv", "products.csv", "orders.csv", "order_items.csv"]


def validate_raw_files():
    for filename in REQUIRED_RAW_FILES:
        path = os.path.join(RAW_DIR, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing expected raw file: {path}")
        if os.path.getsize(path) == 0:
            raise ValueError(f"Raw file is empty: {path}")
    print(f"All {len(REQUIRED_RAW_FILES)} raw files present and non-empty.")


def check_output_quality():
    report_dir = os.path.join(PROCESSED_DIR, "data_quality_report")
    csv_files = [f for f in os.listdir(report_dir) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError(f"No data-quality report CSV found in {report_dir}")

    report_path = os.path.join(report_dir, csv_files[0])
    with open(report_path) as f:
        header = f.readline().strip().split(",")
        values = f.readline().strip().split(",")
        report = dict(zip(header, values))

    valid_rows = int(report.get("valid_rows_output", 0))
    print(f"Data quality report: {report}")
    if valid_rows == 0:
        raise ValueError("Pipeline produced zero valid rows — failing DAG run.")


default_args = {
    "owner": "larissa",
    "retries": 1,
}

with DAG(
    dag_id="ecommerce_sales_pipeline",
    description="Cleans, validates, and aggregates e-commerce sales data with PySpark",
    default_args=default_args,
    schedule=None,  # triggered manually for this portfolio project
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pyspark", "etl", "portfolio"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=f"{sys.executable} {PROJECT_DIR}/generate_data.py",
    )

    validate_raw = PythonOperator(
        task_id="validate_raw",
        python_callable=validate_raw_files,
    )

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command=f"{sys.executable} {PROJECT_DIR}/jobs/process_orders.py",
    )

    check_output = PythonOperator(
        task_id="check_output",
        python_callable=check_output_quality,
    )

    generate_data >> validate_raw >> run_spark_job >> check_output
