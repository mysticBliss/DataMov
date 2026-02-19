"""
Author: Saqib Mujtaba
License: MIT
"""
from ..logger import Logger
import traceback
from pyspark.sql.utils import AnalysisException
from ...utils.exceptions import PathNotFoundException
from typing import Any, Optional, List, Dict, Union
from pyspark.sql import SparkSession, DataFrame

logger = Logger().get_logger()

class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def fetch_data(self, source_type: str, source_path: Optional[Union[str, List[str]]], query: Optional[str] = None, **options: Any) -> Optional[DataFrame]:
        self.spark.sparkContext.setJobDescription("Fetching Data from {} at {} (query={}) ".format(source_type, source_path, query) )
        try:
            if query:
                return self.spark.sql(query)
            else:
                return self.spark.read.format(source_type).options(**options).load(source_path)

        except AnalysisException as ae:
            if "Path does not exist" in str(ae):
                raise PathNotFoundException(
                    "Path '{}' not found: {}".format(source_path, ae))
            else:
                raise ae

        except Exception as e:
            logger.error("Error occurred while fetching data: {}".format(e))
            traceback.print_exc()
            return None

    def create_temp_table_and_resultant_df(self, df: DataFrame, destination_sql: str) -> DataFrame:
        self.spark.sparkContext.setJobDescription("Creating TBL table (query={})".format(destination_sql) )

        logger.debug("Creating TBL table (query={})".format(destination_sql))

        # Temporary Table Name:
        temp_table_name = 'datamov_tmp'
        df.createOrReplaceTempView(temp_table_name)
        generated_sql = "{}  {}".format(destination_sql, temp_table_name)

        logger.info(generated_sql)

        df_transformed = self.spark.sql(generated_sql)

        logger.info("Transformed Schema to Load: {} ".format(df_transformed.printSchema()))

        return df_transformed

    def save_data(self, df: DataFrame, destination_path: Optional[str], format_type: str, mode: str, kudu_masters: Optional[List[str]] = None, table_name: Optional[str] = None, partition_cols: Optional[List[str]] = None) -> Dict[str, Any]:
        if partition_cols is None:
            partition_cols = []

        self.spark.sparkContext.setJobDescription("Save Data of type: {} with mode: {} (table_name={})".format(format_type, mode, table_name) )

        logger.debug("Save Data of type: {} with mode: {} (table_name={})".format(format_type, mode, table_name) )

        try:
            if format_type == "hive":
                if table_name is None:
                    raise ValueError("Table name is required for Hive.")

                try:

                    logger.info("{} df.write.mode({}).partitionBy({}).saveAsTable({})".format(
                        format_type,
                        mode,
                        partition_cols,
                        table_name
                        )

                    )

                    df.write.mode(mode).format("hive").partitionBy(*partition_cols).saveAsTable(table_name)

                except AnalysisException:
                    raise
            elif format_type == "kudu":
                if None in (table_name, kudu_masters):
                    raise ValueError(
                        "Table name and Kudu masters are required for Kudu.")

                if not self._kudu_table_exists(table_name, kudu_masters):
                    logger.error("Kudu table '{}' does not exist.".format(table_name))
                    return {
                        "status": False,
                        "output": df
                    }

                df.write.format("org.apache.kudu.spark.kudu") \
                    .mode(mode) \
                    .option("kudu.table", table_name) \
                    .option("kudu.master", ",".join(kudu_masters)) \
                    .option("kudu.partitionBy", ",".join(partition_cols)) \
                    .save()
            else:
                write_options = {
                    "format": format_type,
                    "mode": mode
                }

                logger.info("df.write.format({}).mode({}).partitionBy({}).save({})".format(
                        format_type,
                        mode,
                        partition_cols,
                        destination_path
                    )
                )

                df.write.format(format_type).mode(mode).partitionBy(*partition_cols).save(destination_path)
            return {
                "status": True,
                "output": df
                }
        except Exception as e:
            logger.error("Error occurred while saving data: {}".format(e))
            traceback.print_exc()
            return {
                "status": False,
                "output": df
                }

    def _kudu_table_exists(self, table_name: str, kudu_masters: List[str]) -> bool:
        """
        Checks if a Kudu table exists by attempting to read its schema.

        Args:
            table_name (str): The name of the Kudu table.
            kudu_masters (List[str]): List of Kudu master addresses.

        Returns:
            bool: True if table exists and is accessible, False otherwise.
        """
        try:
            self.spark.read.format("org.apache.kudu.spark.kudu") \
                .option("kudu.table", table_name) \
                .option("kudu.master", ",".join(kudu_masters)) \
                .load().limit(0).collect()
            return True
        except Exception as e:
            logger.warning("Failed to check existence of Kudu table '{}': {}".format(table_name, e))
            return False
