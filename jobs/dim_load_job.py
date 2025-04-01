from pyspark.sql.functions import col

from common.base_execute import BaseExecute
from common.execute import Execute
from common.conf_loader import load_config
from common.transformations import (
    top_10_countries_by_customers
)

class DimLoadJob(Execute, BaseExecute):
    def execute(self):
        self.run()

    def __init__(self, conf_path: str):
        self.config = load_config(conf_path)
        super().__init__(config=self.config, spark_session=None)

    def run(self):
        print("Starting DimLoadJob...")
        products_conf = self.config["app"]["sources"]["products_raw"]
        customers_conf = self.config["app"]["sources"]["customers_raw"]
        products_path = products_conf["input_path"]
        customers_path = customers_conf["input_path"]

        # Load and validate products
        products = self.spark.read.options(**products_conf["options"]).csv(products_path)
        valid_products = products.filter(col("StockCode").isNotNull() & col("Description").isNotNull())
        invalid_products = products.filter(~(col("StockCode").isNotNull() & col("Description").isNotNull()))

        valid_products.write.mode("overwrite").format("delta").save(self.config["app"]["sources"]["products"]["output_path"])
        invalid_products.write.mode("overwrite").csv("data/errors/products")

        # Load and validate customers
        customers = self.spark.read.options(**customers_conf["options"]).csv(customers_path)
        valid_customers = customers.filter(col("CustomerID").isNotNull() & col("Country").isNotNull())
        invalid_customers = customers.filter(~(col("CustomerID").isNotNull() & col("Country").isNotNull()))

        valid_customers.write.mode("overwrite").format("delta").save(self.config["app"]["sources"]["customers"]["output_path"])
        invalid_customers.write.mode("overwrite").csv("data/errors/customers")

        # Example transformation on customers
        top_countries = top_10_countries_by_customers(valid_customers)
        top_countries.write.mode("overwrite").format("delta").save(self.config["app"]["sources"]["top_ten_countries_for_customers"]["output_path"])
        print("DimLoadJob complete.")
