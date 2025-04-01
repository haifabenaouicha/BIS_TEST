from pyspark.sql.functions import col

from common.base_execute import BaseExecute
from common.execute import Execute
from common.conf_loader import load_config
from common.prepare_functions import prepare_products
from common.transformations import top_10_countries_by_customers

class DimLoadJob(Execute, BaseExecute):
    def execute(self):
        self.run()

    def __init__(self, conf_path: str):
        self.config = load_config(conf_path)
        super().__init__(config=self.config, spark_session=None)

    def run(self):
        self.logger.info("Starting DimLoadJob...")
        products_conf = self.config["app"]["sources"]["products_raw"]
        customers_conf = self.config["app"]["sources"]["customers_raw"]
        products_path = products_conf["input_path"]
        customers_path = customers_conf["input_path"]

        self.logger.info(f"Reading products from: {products_path}")
        products = self.spark.read.options(**products_conf["options"]).csv(products_path)
        self.logger.info("Validating products data...")
        valid_products = self.segregate_valid_from_invalid_data(
            products,
            col("StockCode").isNotNull() &
            col("StockCode").rlike("^[0-9]") &
            col("Description").isNotNull(),
            "data/errors/products"
        )
        self.logger.info("Preparing and writing valid products to parquet.")
        prepare_products(valid_products).coalesce(1).write.mode("overwrite").format("parquet").save(
            self.config["app"]["sources"]["products"]["output_path"]
        )

        self.logger.info(f"Reading customers from: {customers_path}")
        customers = self.spark.read.options(**customers_conf["options"]).csv(customers_path)

        self.logger.info("Validating customers data...")
        valid_customers = self.segregate_valid_from_invalid_data(
            customers,
            col("CustomerID").isNotNull() & col("Country").isNotNull(),
            "data/errors/customers"
        )

        self.logger.info("Writing valid customers to parquet.")
        valid_customers.coalesce(1).write.mode("overwrite").format("parquet").save(
            self.config["app"]["sources"]["customers"]["output_path"]
        )

        self.logger.info("Calculating top 10 countries by customer count...")
        top_countries = top_10_countries_by_customers(valid_customers)

        self.logger.info("Writing top 10 countries result to parquet.")
        top_countries.coalesce(1).write.mode("overwrite").format("parquet").save(
            self.config["app"]["sources"]["top_ten_countries_for_customers"]["output_path"]
        )
        self.logger.info("DimLoadJob complete.")
