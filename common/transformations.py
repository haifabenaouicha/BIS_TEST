from pyspark.sql.functions import avg, sum
from pyspark.sql.functions import col, to_date, month, year, row_number, broadcast
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from common.prepare_functions import prepare_orders, prepare_products


def top_10_countries_by_customers(customers_df):
    return customers_df.groupBy("Country") \
        .agg(countDistinct("CustomerID").alias("num_customers")) \
        .orderBy("num_customers", ascending=False) \
        .limit(10)


def revenue_by_country_streaming(orders_stream_df, products_df, customers_df):
    # Join streaming orders with static products to get UnitPrice
    enriched_orders = orders_stream_df \
        .join(
        broadcast(prepare_products(products_df).select("StockCode", "UnitPrice")),
        on="StockCode",
        how="left"
    )
    # Calculate revenue
    enriched_orders = enriched_orders.withColumn("Revenue", col("Quantity") * col("UnitPrice"))

    # Join with static customers to get Country
    orders_with_country = enriched_orders.join(
        broadcast(customers_df.select("CustomerID", "Country")),
        on="CustomerID",
        how="left"
    )
    # Aggregate revenue per country
    return orders_with_country \
        .groupBy("Country") \
        .agg(sum("Revenue").cast(DoubleType()).alias("TotalRevenue")) \
        .fillna({"TotalRevenue": 0.0})

def price_vs_volume_streaming(orders_stream_df, products_df):
    enriched_orders = prepare_orders(orders_stream_df).join(
        prepare_products(products_df).select("StockCode", "UnitPrice"), on="StockCode", how="inner")

    return enriched_orders.groupBy("StockCode") \
        .agg(
        avg("UnitPrice").alias("AverageUnitPrice"),
        sum("Quantity").alias("TotalQuantitySold")
    )


def top_3_highest_price_products_per_month(orders_stream_df, products_df):
    # Join orders with products
    enriched_orders = prepare_orders(orders_stream_df).join(
        broadcast(prepare_products(products_df).select("StockCode", "UnitPrice")),
        on="StockCode",
        how="left"
    )

    # Format InvoiceDate
    enriched_orders = enriched_orders.withColumn("InvoiceDate", to_date("InvoiceDate", "M/d/yyyy"))

    # Extract year and month
    enriched_orders = enriched_orders \
        .withColumn("Year", year(col("InvoiceDate"))) \
        .withColumn("Month", month(col("InvoiceDate")))

    # Define window partitioned by Year and Month, ordered by UnitPrice descending
    window_spec = Window.partitionBy("Year", "Month").orderBy(col("UnitPrice").desc())

    # Add row number per (Year, Month) group
    ranked_orders = enriched_orders.withColumn("rank", row_number().over(window_spec))

    # Filter top 3 per month
    top_3_products_per_month = ranked_orders.filter(col("rank") <= 3) \
        .select("Year", "Month", "StockCode", "UnitPrice", "rank")
    return top_3_products_per_month
