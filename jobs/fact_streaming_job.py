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
    def execute(self):
        self.run()

    def __init__(self, conf_path: str):
        self.config = load_config(conf_path)
        super().__init__(config=self.config, spark_session=None)

    def run(self):
        self.logger.info("Starting FactLoadJob...")

        orders_conf = self.config["app"]["sources"]["orders_raw"]
        orders_path = orders_conf["input_path"]

        self.logger.info(f"Reading streaming orders data from: {orders_path}")
        orders_stream = self.spark.readStream.options(**orders_conf["options"]).csv(orders_path)

        self.logger.info("Preparing orders data...")
        prepared_orders = prepare_orders(orders_stream)

        self.logger.info("Reading enriched products and customers from output parquet files...")
        products = self.spark.read.parquet(self.config["app"]["sources"]["products"]["output_path"])
        customers = self.spark.read.parquet(self.config["app"]["sources"]["customers"]["output_path"])
        self.logger.info("Calculating revenues by country...")
        revenue_by_country = revenue_by_country_streaming(prepared_orders, products, customers)
        self.logger.info("Calculating price_vs_volume_value by country...")
        price_vs_volume_value = price_vs_volume_streaming(prepared_orders, products)

        self.logger.info("Calculating top_3_highest_price_products_per_month...")
        top_products = top_3_highest_price_products_per_month(prepared_orders, products)

        # Writing logic will be handled separately
        self.logger.info("FactLoadJob transformations complete. Ready for write step.")
        from pyspark.sql.streaming import DataStreamWriter

        self.logger.info("Writing revenue_by_country to Delta...")
        revenue_by_country.writeStream \
            .format("delta") \
            .outputMode("complete") \
            .option("checkpointLocation", "checkpoints/revenue_by_country") \
            .option("path", "data/output/revenue_by_country") \
            .trigger(once=True) \
            .start()

        self.logger.info("Writing price_vs_volume_value to Delta...")
        price_vs_volume_value.writeStream \
            .format("delta") \
            .outputMode("complete") \
            .option("checkpointLocation", "checkpoints/price_vs_volume") \
            .option("path", "data/output/price_vs_volume") \
            .trigger(once=True) \
            .start()

        self.logger.info("Writing top_3_highest_price_products_per_month to Delta...")
        top_products.writeStream \
            .format("delta") \
            .outputMode("complete") \
            .option("checkpointLocation", "checkpoints/top_3_products") \
            .option("path", "data/output/top_3_products") \
            .trigger(once=True) \
            .start()

