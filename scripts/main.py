import json
from datamov import DataMovements, Logger, Engine
import argparse


logger = Logger().get_logger()

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Execute data flow with specified parameters")
    parser.add_argument("-f", "--flow_name", help="Flow name", required=True)
    parser.add_argument("-c", "--spark_config",
                        help="Spark Configuration", required=False)
    return parser.parse_args()


def load_data_and_execute_flow(flow_name, spark_config):
    dm = DataMovements()
    cfg = dm.environment_configs
    logger.info("{}".format(dm.data_movements))
    flow = dm.data_movements[flow_name]

    executor = Engine(spark_config)
    executor.load_data_flow(flow, cfg)
    executor.run_flow()


if __name__ == "__main__":
    args = parse_arguments()
    if args.spark_config:
        spark_config = json.loads(args.spark_config.replace("'", "\""))
        print(spark_config)

    load_data_and_execute_flow(args.flow_name, args.spark_config)