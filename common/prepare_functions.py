from pyspark.sql.functions import col, to_timestamp, trim

def write_invalid_rows(df, conditions, output_path, name="dataset"):
    """
    Filters and writes invalid rows based on provided conditions.

    Parameters:
    - df: Input DataFrame
    - conditions: PySpark condition expression (boolean mask)
    - output_path: Directory to write invalid rows to
    - name: Optional label for logging
    """
    invalid_df = df.filter(conditions)
    valid_df = df.filter(~conditions)
    count = invalid_df.count()

    if count > 0:
        print(f"{count} invalid rows found in {name}. Writing to {output_path}")
        invalid_df.write.mode("overwrite").csv(output_path)
    else:
        print(f"No invalid rows in {name}.")
    return valid_df

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

