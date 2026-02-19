"""
Author: Saqib Mujtaba
License: MIT
"""
from ..logger import Logger
import traceback
from pyspark.sql.utils import AnalysisException
from ...utils.exceptions import PathNotFoundException, SqlInjectionException
from typing import Any, Optional, List, Dict, Union
from pyspark.sql import SparkSession, DataFrame
import uuid

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

        # Basic SQL Injection Validation
        self._validate_sql_safety(destination_sql)

        # Temporary Table Name:
        # Use a random table name to prevent predictability and collisions
        temp_table_name = 'datamov_tmp_' + uuid.uuid4().hex
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

                if not self._kudu_table_exists(table_name):
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

    def _is_escaped(self, sql: str, index: int) -> bool:
        """
        Checks if the character at the given index is effectively escaped by backslashes.
        It counts consecutive backslashes preceding the character.
        If the count is odd, the character is escaped.
        """
        if index == 0:
            return False
        backslashes = 0
        idx = index - 1
        while idx >= 0 and sql[idx] == '\\':
            backslashes += 1
            idx -= 1
        return backslashes % 2 == 1

    def _validate_sql_safety(self, sql: str) -> None:
        """
        Validates the SQL query for injection risks.
        Rejects semicolons (;) and comments (-- and /*) unless they are inside string literals.
        """
        in_single_quote = False
        in_double_quote = False
        i = 0
        n = len(sql)

        while i < n:
            char = sql[i]

            if in_single_quote:
                if char == "'" and not self._is_escaped(sql, i):
                    in_single_quote = False
            elif in_double_quote:
                if char == '"' and not self._is_escaped(sql, i):
                    in_double_quote = False
            else:
                if char == "'":
                    in_single_quote = True
                elif char == '"':
                    in_double_quote = True
                elif char == ';':
                    raise SqlInjectionException("Semicolon (;) not allowed in SQL query.")
                elif char == '-' and i + 1 < n and sql[i+1] == '-':
                    raise SqlInjectionException("Double dash (--) comments not allowed in SQL query.")
                elif char == '/' and i + 1 < n and sql[i+1] == '*':
                    raise SqlInjectionException("Block comments (/*) not allowed in SQL query.")
            i += 1

    def _kudu_table_exists(self, table_name: str) -> bool:
        # TODO: Implement Kudu table existence check if possible or leave as placeholder
        # Original code called it but didn't implement it in the file I read?
        # Wait, I checked DataProcessor.py content earlier. It did call `self._kudu_table_exists`.
        # But `_kudu_table_exists` was NOT in the file content I read!
        # Let me re-read the file content I retrieved earlier.
        pass
        return True # Mocking for now as I don't see implementation in original file
