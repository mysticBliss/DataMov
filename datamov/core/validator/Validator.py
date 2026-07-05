import great_expectations as gx
from pyspark.sql import DataFrame
from typing import List, Dict, Any, Optional
import uuid
import traceback

from ...core.logger import Logger

logger = Logger().get_logger()

class Validator:
    def __init__(self, df: DataFrame):
        self.df = df
        self.context = gx.get_context()
        self.datasource_name = "spark_datasource_{}".format(uuid.uuid4())
        self.asset_name = "spark_asset"
        self.suite_name = "validation_suite_{}".format(uuid.uuid4())

    def validate(self, expectations: List[Dict[str, Any]]) -> bool:
        if not expectations:
            logger.info("No expectations provided. Skipping validation.")
            return True

        logger.info("Starting validation with {} expectations.".format(len(expectations)))

        try:
            # 1. Add Datasource
            datasource = self.context.data_sources.add_or_update_spark(self.datasource_name)

            # 2. Add Data Asset
            data_asset = datasource.add_dataframe_asset(name=self.asset_name)

            # 3. Create Expectation Suite
            try:
                self.context.suites.get(self.suite_name)
            except Exception:
                self.context.suites.add(gx.ExpectationSuite(name=self.suite_name))

            # 4. Build Batch Request
            batch_request = data_asset.build_batch_request(options={"dataframe": self.df})

            # 5. Get Validator
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=self.suite_name
            )

            # 6. Run Expectations
            all_passed = True
            for expectation in expectations:
                exp_type = expectation.get("type")
                kwargs = expectation.get("kwargs", {})

                if not exp_type:
                    logger.warning("Expectation definition missing 'type': {}".format(expectation))
                    continue

                method = getattr(validator, exp_type, None)
                if method:
                    logger.info("Running expectation: {} with kwargs: {}".format(exp_type, kwargs))
                    try:
                        result = method(**kwargs)
                        if not result["success"]:
                            logger.error("Expectation failed: {} result: {}".format(exp_type, result))
                            all_passed = False
                        else:
                            logger.info("Expectation passed: {}".format(exp_type))
                    except Exception as e:
                        logger.error("Error executing expectation {}: {}".format(exp_type, e))
                        all_passed = False
                else:
                    logger.warning("Expectation type '{}' not found in Great Expectations validator.".format(exp_type))
                    all_passed = False

            return all_passed

        except Exception as e:
            logger.error("Error during Great Expectations validation: {}".format(e))
            traceback.print_exc()
            return False
