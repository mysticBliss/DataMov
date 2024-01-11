"""
Author: Saqib Mujtaba
License: MIT
"""
from ..logger import Logger
import traceback
from pyspark.sql.utils import AnalysisException
from ...utils.exceptions import PathNotFoundException
from typing import Any, Optional


logger = Logger().get_logger()

class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def fetch_data(self, source_type, source_path, query=None, **options):
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

    def create_temp_table_and_resultant_df(self, df, destination_sql):
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

    def save_data(self, df, destination_path, format_type, mode, kudu_masters=None, table_name=None, partition_cols=[]):
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
                    return False

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
                # https://spark.apache.org/docs/2.4.3/api/python/pyspark.sql.html#pyspark.sql.DataFrame
                # As per the pyspark code; current only JSON and PARQUET is supported

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

