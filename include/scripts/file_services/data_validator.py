from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import DataContext
from include.scripts.utils.config import config, settings
from include.scripts import APP_PATH


class DataValidator:
    def __init__(self):
        self.s3_bucket_name = config.s3_bucket_name
        self.context_root_dir = "/".join([APP_PATH, config.context_root_dir])
        self.ge_snowflake_datasource = config.ge_snowflake_datasource

    def validate_staging_table_data(self, entity):
        data_context: DataContext = DataContext(context_root_dir=self.context_root_dir)
        runtime_param_dict = {"query": "select * from {0}.t_{1}".format(config.snowflake_stage_schema, entity)}
        batch_identifier_name = "_".join([entity, 'staging_table'])
        batch_request = RuntimeBatchRequest(
            datasource_name=self.ge_snowflake_datasource,
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="_".join([entity, 'staging_table']),
            runtime_parameters=runtime_param_dict,
            batch_identifiers={"default_identifier_name": batch_identifier_name},
        )
        validator = data_context.get_validator(
            batch_request=batch_request, expectation_suite_name="_".join([entity, 'data_expectations'])
        )
        print(entity)
        result: CheckpointResult = data_context.run_checkpoint(
            checkpoint_name="_".join([entity, 'checkpoint']),
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": "_".join([entity, 'data_expectations'])
                }
            ]
        )
        validator.execution_engine.close()
        return result["success"]


