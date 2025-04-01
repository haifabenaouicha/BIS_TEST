# BIS_TEST: Spark ETL Project

## Overview

**BIS_TEST** is a modular PySpark-based ETL framework for processing and transforming e-commerce data. The project supports both batch and streaming workflows (streaming workflows didn't work for me so I kept everything in batch), utilizes Delta Lake for reliability, and is structured for easy deployment on platforms like Cloudera.

---

## Project Structure

```
BIS_TEST/
├── common/                  # Shared logic: Spark setup, utilities, validators
├── jobs/                    # ETL jobs (DimLoadJob and FactLoadJob)
├── conf/                    # Configuration files (job.conf)
├── data/                    # Input/output data folders (raw and processed)
├── tests/                   # Unit tests for transformations and functions
├── main.py                  # Entry point to execute jobs
├── setup.py                 # Packaging setup for pip/wheel
├── Pipfile / Pipfile.lock   # Pipenv environment management
├── README.md                # Project documentation
├── run.sh                   # Spark submit helper script
```

---

## Configuration (`conf/job.conf`)

Configuration is written in HOCON format and contains all Spark and source data settings.

### Example config block:

```hocon
app {
  environment = "local"
  spark = {
    app_name = "BISApp"
    master = "local[*]"
    options = {
      "spark.sql.debug.maxToStringFields" = 1000
      "spark.sql.adaptive.enabled" = true
      "spark.sql.autoBroadcastJoinThreshold" = 104857600
      "spark.sql.extensions" = "io.delta.sql.DeltaSparkSessionExtension"
      "spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }
  }
  sources {
    products_raw.input_path = "data/raw/products.csv"
    customers_raw.input_path = "data/raw/customers.csv"
    orders_raw.input_path = "data/raw/orders"
    products.output_path = "data/output/products"
    customers.output_path = "data/output/customers"
    top_ten_countries_for_customers.output_path = "data/output/top_countries"
  }
}
```

---

## Jobs Explained

### `DimLoadJob`

**Purpose:** Batch process and validate static datasets (products, customers), then prepare and store them in optimized Parquet format.

**Steps:**

- Reads products and customers from raw CSVs.
- Validates that:
  - `StockCode` is not null and starts with a digit.
  - `Description` is not null.
  - `CustomerID` and `Country` are not null.
- Segregates invalid rows and stores them in `data/errors/`.
- Prepares product data using transformation logic.
- Writes clean data as Parquet.
- Computes and stores top 10 countries by customer count.

---

### `FactLoadJob`
**Please note streaming approach didn't work for me and I was too tired to make it run smoothly**

**Purpose:** Process real-time orders data using Spark Structured Streaming and join with existing dimensional data for analytics.

**Stream-Static Join:**
Since new order files arrive hourly, the job performs a **stream-to-static join** where streaming orders are joined with static `products` and `customers` datasets stored as Parquet.

**Steps:**

- Reads streaming orders from raw CSV folder using Spark Structured Streaming.
- Prepares the orders using transformation logic.
- Loads previously prepared `products` and `customers` from Parquet.
- Joins the stream with static dimensions.
- Computes:
  - Total sales by country
  - Average transaction value by country
  - Top 3 highest price products per month

**Writing Output:**
- Results are written to **Delta Lake** in `outputMode("complete")`.
- Output is **overwritten at each trigger** (batch-style streaming) using `trigger(once=True)`.
- This setup is optimal for hourly batch-style streaming and simplifies integration with downstream BI tools or dashboards.

---

## Pipenv Setup

This project uses **Pipenv** for managing dependencies.

### Installing dependencies:

```bash
pip install pipenv
pipenv install --dev
```

### Running in virtual environment:

```bash
pipenv shell
```

---

## Running Locally

### Option 1: Pipenv Shell + Python

```bash
pipenv shell
python main.py conf/job.conf
```

### Option 2: Spark Submit

```bash
spark-submit \
  --master local[*] \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --packages io.delta:delta-core_2.12:2.4.0 \
  main.py conf/job.conf
```

---

## Packaging as a Wheel

### Building the wheel

```bash
pipenv run python setup.py bdist_wheel
```

### Installing the wheel on Cloudera

```bash
pip install dist/BIS_TEST-0.1.0-py3-none-any.whl
```

---

## CI/CD with GitHub Actions

A GitHub Actions workflow automates:

- Linting (PEP8 via flake8)
- Running unit tests
- Checking code coverage
- Building the wheel package

### Sample Workflow: `.github/workflows/ci.yml`

```yaml
name: CI Pipeline
on: [push, pull_request]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with: { python-version: '3.9' }
      - run: pip install pipenv
      - run: pipenv install --dev
      - run: pipenv run flake8 BIS_TEST/
      - run: pipenv run pytest --cov=BIS_TEST --cov-report=xml
      - run: pipenv run python setup.py bdist_wheel
      - uses: actions/upload-artifact@v3
        with:
          name: bis_jobs_wheel
          path: dist/*.whl
```

---

## Testing

All unit tests are located under the `tests/` directory.

Run with:

```bash
pipenv run pytest
```

---

## Notes

- You should **not package** `job.conf` in the wheel — keep it external and configurable.
- Make sure data files (CSV) are available in the specified paths when running locally.

---

## Author

**Haifa Ben Aouicha**


