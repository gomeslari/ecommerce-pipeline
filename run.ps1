# Sets up the local environment and runs the PySpark ETL job.
# Usage (PowerShell): .\run.ps1

$env:JAVA_HOME = "C:\Users\larissagomes\.jdks\corretto-17.0.16"
$env:HADOOP_HOME = "C:\Users\larissagomes\dev\hadoop"
$env:PATH = "$env:PATH;$env:HADOOP_HOME\bin"
$env:PYSPARK_PYTHON = "C:\Users\larissagomes\dev\ecommerce-pipeline-venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Users\larissagomes\dev\ecommerce-pipeline-venv\Scripts\python.exe"

& $env:PYSPARK_PYTHON (Join-Path $PSScriptRoot "jobs\process_orders.py")
