"""
Author: Saqib Mujtaba
License: MIT
"""

from ...core.data_processor import DataProcessor
from ...connectors import SparkManager
from ..logger import Logger
from ..data_flow import DataFlow
from ..data_movements.DataMovements import EnvironmentConfig
from ..validator.Validator import Validator
from ...utils.exceptions import FlowTypeException, CreateTrackingDB, SqlNotFound
from pyspark.sql.functions import lit
from datetime import date, timedelta
from typing import Dict, Any, Optional

logger = Logger().get_logger()

class DataSystems:
    HIVE = 'hive'
    KUDU = 'kudu'
    IMPALA = 'impala'
    PARQUET = 'parquet'


class Engine:
    def __init__(self, spark_config: Optional[Dict[str, Any]] = None):
        self.dataflow: Dict[DataFlow, Any] = {}
        self.config = spark_config if spark_config else {}

    def load_data_flow(self, current_flow: DataFlow, environments: Any) -> None:
        if not isinstance(current_flow, DataFlow):
            raise FlowTypeException()

        self.dataflow[current_flow] = environments

        logger.info("ENGINE: Data Movement Definition: %s", self.dataflow)

    def run_flow(self) -> None:

        for flow, envs in self.dataflow.items():
            logger.info(
                "ENGINE: Starting Flow Execution: {}".format(flow.name))

            # Fixing None Attributes
            MODE = getattr(flow, 'destination_mode', None)
            PATH = getattr(flow, 'destination_path', None)
            PARTITIONS = getattr(flow, 'destination_partitions', None)

            app_name = "DataFlow: {}".format(flow.name)


            with SparkManager(app_name, config=self.config) as spark:
                data_processor = DataProcessor(spark)

                # Create Tracking DB exists [datamov_monitoring_db]
                if not spark.catalog._jcatalog.databaseExists("datamov_monitoring_db"):
                     try:
                        if not [db for db in spark.catalog.listDatabases() if db.name == "datamov_monitoring_db"]:
                            raise CreateTrackingDB
                     except Exception:
                         try:
                             if not spark.catalog._jcatalog.databaseExists("datamov_monitoring_db"):
                                 raise CreateTrackingDB
                         except Exception:
                             pass

                # https://stackoverflow.com/questions/58633753/ignoring-non-spark-config-property-hive-exec-dynamic-partition-mode
                spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")


                if flow.source_type and flow.source_type.lower() in {DataSystems.HIVE, DataSystems.IMPALA, DataSystems.KUDU}:
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
                    if df:
                        logger.info("ENGINE: Fetched {} records from {}".format(
                            df.count(), flow.source_type))
                    else:
                        logger.warning("ENGINE: Failed to fetch data from {}".format(flow.source_type))
                        continue

                if df is None:
                    continue

                df_transformed = data_processor.create_temp_table_and_resultant_df(
                    df, flow.destination_sql
                    )

                logger.info("ENGINE: Fetched {} records from {}"
                            .format(
                                df_transformed.count(),
                                flow.source_type
                                )
                )

                # Validation Step
                if flow.expectations:
                    logger.info("ENGINE: Running Great Expectations validation...")
                    validator = Validator(df_transformed)
                    validation_success = validator.validate(flow.expectations)

                    if not validation_success:
                        logger.error("ENGINE: Validation failed for flow {}. Skipping save operation.".format(flow.name))
                        # Optionally record failure in tracking DB here?
                        # For now, just skip.
                        continue

                logger.info(
                    "Loading {} from TEMP_TABLE in {} mode at location {} with partitions -> {}".format(
                        flow.destination_type,
                        MODE,
                        PATH,
                        PARTITIONS
                    )
                )

                count_transformed = df_transformed.count()

                if count_transformed > 0:

                    # generate uuid for this tracking process
                    GENERATED_UUID = DataFlow.generate_tracking_id()

                    df_transformed = df_transformed.withColumn('uuid', lit(GENERATED_UUID))


                    STATUS_DICT = data_processor.save_data(
                        df=df_transformed,
                        destination_path=PATH,
                        format_type=flow.destination_type, # type: ignore
                        mode=MODE, # type: ignore
                        kudu_masters=None,
                        table_name=flow.destination_table,
                        partition_cols=PARTITIONS
                    )

                    # Final Loaded DataFrame
                    df_final_load = STATUS_DICT.get("output")

                    # Populate ETLTracking table
                    TRACKING_DICT = flow.to_dict() # dictionary object for the class


                    TRACKING_DICT['dm_etl_uuid'] = GENERATED_UUID
                    TRACKING_DICT['source_count'] = count_transformed
                    TRACKING_DICT['destination_count'] = df_final_load.count() if df_final_load else 0
                    TRACKING_DICT['load_status'] = 'SUCCESS' if STATUS_DICT.get("status") else "FAILED"
                    logger.info("TRACKING_DICT: {}".format(TRACKING_DICT))


                    try:
                        keys = list(TRACKING_DICT.keys())
                        values = [TRACKING_DICT[k] for k in keys]

                        df_tracking = spark.createDataFrame([tuple(values)], schema=keys)

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
                    except Exception as e:
                        logger.error("Failed to save tracking info: {}".format(e))

                else:
                    logger.warning(
                        "ENGINE: No Data to Load: {}".format(flow.name))

        logger.info("ENGINE: Exiting Flow Execution: {}".format(flow.name))
