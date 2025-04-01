import os
import pytest
from pyspark.sql import SparkSession

ROOT_DIR = os.path.dirname(__file__)  # points to 'tests/'

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("PySparkUnitTests") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def customers_df(spark):
    path = os.path.join(ROOT_DIR, "assets", "customers.csv")
    return spark.read.option("header", True).csv(path)

@pytest.fixture
def products_df(spark):
    path = os.path.join(ROOT_DIR, "assets", "products.csv")
    return spark.read.option("header", True).csv(path)

@pytest.fixture
def orders_df(spark):
    path = os.path.join(ROOT_DIR, "assets", "orders.csv")
    return spark.read.option("header", True).csv(path)
