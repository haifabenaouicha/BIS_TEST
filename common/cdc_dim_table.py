from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from base_execute import BaseExecute
from pyspark.sql.utils import AnalysisException

# for now this class is not used , it will be called if we decide to use CDC for dim tables and upsert the dim tbales data daily
class CdcDimTable(BaseExecute):
    def __init__(self, table_path: str, primary_key: str, spark):
        super().__init__(spark)
        self.table_path = table_path
        self.primary_key = primary_key

        # Try to load the Delta table if it exists
        try:
            self.delta_table = DeltaTable.forPath(spark, table_path)
        except AnalysisException:
            self.delta_table = None  # Table will be created on first write

    def upsert(self, updates_df: DataFrame):
        """
        Apply CDC using a MERGE (UPSERT) operation.
        """
        if self.delta_table is None:
            # First time: write full data
            updates_df.write.format("delta").mode("overwrite").save(self.table_path)
            self.delta_table = DeltaTable.forPath(self.spark, self.table_path)
            return

        # Perform MERGE
        alias_target = "target"
        alias_source = "jobs"
        join_condition = f"{alias_target}.{self.primary_key} = {alias_source}.{self.primary_key}"

        self.delta_table.alias(alias_target).merge(
            updates_df.alias(alias_source),
            join_condition
        ).whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
