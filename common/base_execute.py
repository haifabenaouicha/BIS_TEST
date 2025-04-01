import logging
from pyspark.sql import SparkSession

class BaseExecute:
    def __init__(self, config, spark_session=None):
        self.config = config
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

        env = config["app"]["environment"]

        if spark_session:
            self.spark = spark_session
            self.logger.info("Using existing SparkSession.")
        else:
            if env == "local":
                self.logger.info("Initializing local SparkSession with config overrides.")
                spark_builder = SparkSession.builder.appName(config["app"]["spark"]["app_name"])
                for k, v in config["app"]["spark"]["options"].items():
                    spark_builder = spark_builder.config(k, v)
                if config["app"]["spark"].get("delta_extensions", False):
                    spark_builder = spark_builder \
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                self.spark = spark_builder.getOrCreate()
            else:
                self.logger.info("Initializing SparkSession for cluster (no config overrides).")
                self.spark = SparkSession.builder.getOrCreate()

    def segregate_valid_from_invalid_data(self, df, condition, error_path):
        """
        Filters valid/invalid rows from a DataFrame based on a condition.
        Writes invalid rows to the specified path only if they exist.
        Returns valid rows.
        """
        valid_rows = df.filter(condition)
        invalid_rows = df.filter(~condition)

        invalid_count = invalid_rows.count()
        if invalid_count > 0:
            self.logger.info(f"Writing {invalid_count} invalid rows to {error_path}")
            invalid_rows.write.mode("overwrite").csv(error_path)
        else:
            self.logger.info("No invalid rows to write.")

        return valid_rows
