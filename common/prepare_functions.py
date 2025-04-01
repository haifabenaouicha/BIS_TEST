from pyspark.sql.functions import col, to_timestamp, trim

def prepare_orders(orders_df):
    """
    Cast and clean raw orders DataFrame to match fact_orders schema.
    """
    return orders_df \
        .withColumn("InvoiceNo", col("InvoiceNo").cast("string")) \
        .withColumn("StockCode", col("StockCode").cast("string")) \
        .withColumn("Quantity", col("Quantity").cast("int")) \
        .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")) \
        .withColumn("CustomerID", col("CustomerID").cast("string"))
def prepare_products(products_df):
    """
    Clean and cast raw products DataFrame to match dim_products schema.
    """
    return products_df \
        .withColumn("StockCode", col("StockCode").cast("string")) \
        .withColumn("Description", trim(col("Description")).cast("string")) \
        .withColumn("UnitPrice", col("UnitPrice").cast("double"))

