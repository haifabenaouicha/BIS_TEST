from pyspark.sql.functions import to_timestamp, col

# Project-specific imports
from common.base_execute import BaseExecute
from common.conf_loader import load_config
from common.execute import Execute
from common.prepare_functions import prepare_orders
from common.transformations import (
    revenue_by_country_streaming,
    price_vs_volume_streaming,
    top_3_highest_price_products_per_month
)


class FactLoadJob(Execute, BaseExecute):
    def __init__(self, conf_path: str):
        # Load configuration and initialize Spark session
        self.config = load_config(conf_path)
        super().__init__(config=self.config, spark_session=None)

    @staticmethod
    def get_orders_schema():
        # Define schema for reading raw orders CSV (all columns as string)
        from pyspark.sql.types import StructType, StructField, StringType
        return StructType([
            StructField("InvoiceNo", StringType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Quantity", StringType(), True),
            StructField("InvoiceDate", StringType(), True),
            StructField("CustomerID", StringType(), True)
        ])

    def run(self):
        self.logger.info("Starting FactLoadJob in batch mode...")

        # Load config for orders input
        orders_conf = self.config["app"]["sources"]["orders_raw"]
        orders_path = orders_conf["input_path"]

        # Read orders CSV file as batch with schema
        self.logger.info(f"Reading orders batch data from: {orders_path}")
        orders_df = self.spark.read \
            .schema(self.get_orders_schema()) \
            .options(**orders_conf["options"]) \
            .csv(orders_path)

        # Parse timestamp and prepare cleaned orders DataFrame
        self.logger.info("Preparing orders data...")
        orders_df = orders_df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
        prepared_orders = prepare_orders(orders_df)

        # Read reference dimension tables (batch-parquet)
        self.logger.info("Reading products and customers dimensions...")
        products = self.spark.read.parquet(self.config["app"]["sources"]["products"]["output_path"])
        customers = self.spark.read.parquet(self.config["app"]["sources"]["customers"]["output_path"])

        # Apply analytics transformation: total revenue by country
        self.logger.info("Running transformation: revenue_by_country...")
        revenue_by_country = revenue_by_country_streaming(prepared_orders, products, customers)

        # Apply analytics transformation: price vs volume per country
        self.logger.info("Running transformation: price_vs_volume...")
        price_vs_volume_value = price_vs_volume_streaming(prepared_orders, products)

        # Apply analytics transformation: top 3 high-price products per month
        self.logger.info("Running transformation: top_3_highest_price_products_per_month...")
        top_products = top_3_highest_price_products_per_month(prepared_orders, products)

        # Write transformed outputs as Parquet (overwrite mode)
        self.logger.info("Writing revenue_by_country to Parquet...")
        revenue_by_country.write.mode("overwrite").parquet("data/output/revenue_by_country")

        self.logger.info("Writing price_vs_volume to Parquet...")
        price_vs_volume_value.write.mode("overwrite").parquet("data/output/price_vs_volume")

        self.logger.info("Writing top_3_products to Parquet...")
        top_products.write.mode("overwrite").parquet("data/output/top_3_products")

        self.logger.info("FactLoadJob (batch mode) complete.")

    def execute(self):
        # Entrypoint method to trigger the job
        self.run()
