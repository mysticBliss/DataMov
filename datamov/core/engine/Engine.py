"""
Author: Saqib Mujtaba
License: MIT
"""

from ...core.data_processor import DataProcessor
from ...connectors import SparkManager
from ..logger import Logger
from ..data_flow import DataFlow
from ..data_movements.DataMovements import EnvironmentConfig
from ...utils.exceptions import FlowTypeException, CreateTrackingDB, SqlNotFound
from pyspark.sql.functions import lit
from datetime import date, timedelta

logger = Logger().get_logger()

class DataSystems:
    HIVE = 'hive'
    KUDU = 'kudu'
    IMPALA = 'impala'
    PARQUET = 'parquet'


class Engine:
    def __init__(self, spark_config=None):
        self.dataflow = {}
        self.config = spark_config if spark_config else {}

    def load_data_flow(self, current_flow, environments):
        if not isinstance(current_flow, DataFlow):
            raise FlowTypeException()

        self.dataflow[current_flow] = environments

        logger.info("ENGINE: Data Movement Definition: %s", self.dataflow)

    def run_flow(self):
        
        for flow, envs in self.dataflow.items():
            logger.info(
                "ENGINE: Starting Flow Execution: {}".format(flow.name))

            # Fixing None Attributes
            MODE = getattr(flow, 'destination_mode', None)
            PATH = getattr(flow, 'destination_path', None)
            PARTITIONS = getattr(flow, 'destination_partitions', None)

            app_name = "DataFlow: {}".format(flow.name)
            

            with SparkManager(app_name, config=None) as spark:
                data_processor = DataProcessor(spark)

                # Create Tracking DB exists [datamov_monitoring_db]
                if not spark.catalog._jcatalog.databaseExists("datamov_monitoring_db"):
                    raise CreateTrackingDB
                
                # https://stackoverflow.com/questions/58633753/ignoring-non-spark-config-property-hive-exec-dynamic-partition-mode
                spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")


                if flow.source_type.lower() in {DataSystems.HIVE, DataSystems.IMPALA, DataSystems.KUDU}:
                    if flow.source_sql is None:
                        raise SqlNotFound
                    df = data_processor.fetch_data(
                        flow.source_type,
                        source_path=None,
                        query=flow.source_sql
                    )
                else:
                    df = data_processor.fetch_data(
                        flow.source_type,
                        flow.generate_paths,
                        query=None
                    )
                    logger.info("ENGINE: Fetched {} records from {}".format(
                        df.count(), flow.source_type))

                df_transformed = data_processor.create_temp_table_and_resultant_df(
                    df, flow.destination_sql
                    )

                logger.info("ENGINE: Fetched {} records from {}"
                            .format(
                                df_transformed.count(),
                                flow.source_type
                                )
                )   

                logger.info(
                    "Loading {} from TEMP_TABLE in {} mode at location {} with partitions -> {}".format(
                        flow.destination_type,
                        MODE,
                        PATH,
                        PARTITIONS
                    )
                )
                if df_transformed.count() > 0:

                    # generate uuid for this tracking process
                    GENERATED_UUID = DataFlow.generate_tracking_id()

                    df_transformed = df_transformed.withColumn('uuid', lit(GENERATED_UUID))


                    STATUS_DICT = data_processor.save_data(
                        df=df_transformed,
                        destination_path=PATH,
                        format_type=flow.destination_type,
                        mode=MODE,
                        kudu_masters=None,
                        table_name=flow.destination_table,
                        partition_cols=PARTITIONS 
                    )
                     
                    # Final Loaded DataFrame
                    df_final_load = STATUS_DICT.get("output")
                        
                    # Populate ETLTracking table
                    TRACKING_DICT = flow.to_dict() # dictionary object for the class


                    TRACKING_DICT['dm_etl_uuid'] = GENERATED_UUID
                    TRACKING_DICT['source_count'] = df_transformed.count()
                    TRACKING_DICT['destination_count'] = df_final_load.count()
                    TRACKING_DICT['load_status'] = 'SUCCESS' if STATUS_DICT.get("status") else "FAILED"
                    logger.info("TRACKING_DICT: {}".format(TRACKING_DICT))
                    

                    df_tracking = spark.createDataFrame(
                         [tuple(TRACKING_DICT.values())] , schema=TRACKING_DICT.keys()
                        )
                    
                    df_tracking.select('dm_etl_uuid', 'load_status').show()

                    data_processor.save_data(
                        df=df_tracking,
                        destination_path=None,
                        format_type="hive",
                        mode="append",
                        kudu_masters=None,
                        table_name="datamov_monitoring_db.t_etl_flow_tracker",
                        partition_cols=[] 
                    )

                else:
                    logger.warning(
                        "ENGINE: No Data to Load: {}".format(flow.name))

        logger.info("ENGINE: Exiting Flow Execution: {}".format(flow.name))

