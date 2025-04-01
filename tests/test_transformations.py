from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.functions import round, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType
from common.transformations import (
    top_10_countries_by_customers,
    revenue_by_country_streaming,
    price_vs_volume_streaming,
    top_3_highest_price_products_per_month
)


def test_top_10_countries(customers_df, spark_session):
    # Run the transformation function
    result_df = top_10_countries_by_customers(customers_df)

    # Define the expected schema explicitly
    expected_schema = StructType([
        StructField("Country", StringType(), True),
        StructField("num_customers", LongType(), True),
    ])

    # Define expected data
    expected_data = [
        ("Finland", 1),
        ("Italy", 1),
        ("Iceland", 1),
        ("United Kingdom", 1),
    ]

    expected_df = spark_session.createDataFrame(expected_data, schema=expected_schema)

    # Compare full data and schema using chispa
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_row_order=True)


def test_revenue_by_country_streaming(spark, orders_df, products_df, customers_df):
    # Run the transformation function
    result_df = revenue_by_country_streaming(orders_df, products_df, customers_df)

    # Define the expected schema
    expected_schema = StructType([
        StructField("Country", StringType(), True),
        StructField("TotalRevenue", DoubleType(), True),
    ])

    # Define expected data with correct values and None for null
    expected_data = [
        ("Finland", 192.0),
        ("Italy", 11.82),
        ("Iceland", 0.0),
        ("United Kingdom", 0.0),
    ]

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    expected_df = expected_df.withColumn("TotalRevenue", round(col("TotalRevenue"), 2))
    # Compare both data and schema using chispa
    result_df.show()
    assert_df_equality(
        result_df,
        expected_df,
        ignore_nullable=True,
        ignore_row_order=True

    )
def test_price_vs_volume_streaming(spark, orders_df, products_df):
    # Run the transformation function
    result_df = price_vs_volume_streaming(orders_df, products_df)

    # Define the expected schema
    expected_schema = StructType([
        StructField("StockCode", StringType(), True),
        StructField("AverageUnitPrice", DoubleType(), True),
        StructField("TotalQuantitySold", LongType(), True),
    ])

    # Define expected data
    expected_data = [
        ("21533", 3.94, 3),
        ("23166", 2.69, 0),
        ("21725", 4.00, 48),
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Compare both data and schema using chispa
    assert_df_equality(
        result_df,
        expected_df,
        ignore_nullable=True,
        ignore_row_order=True
    )
def test_top_3_price_drop_last_month_from_orders(spark, orders_df, products_df):
    # Run the transformation function
    result_df = top_3_highest_price_products_per_month(orders_df, products_df)

    # Define expected schema
    expected_schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("StockCode", StringType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("rank", IntegerType(), False),
    ])

    # Define expected data
    expected_data = [
        (2010, 12, "21725", 4.00, 1),
        (2010, 12, "21211", None, 2),
        (2011, 1,  "23166", 2.69, 1),
        (2011, 1,  "23166", 2.69, 2),
        (2011, 4,  "20665", None, 1),
        (2011, 11, "21533", 3.94, 1),
        (2011, 11, "21086", None, 2),
    ]

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Compare actual vs expected DataFrames
    assert_df_equality(
        result_df,
        expected_df,
        ignore_nullable=True,
        ignore_row_order=True
    )
