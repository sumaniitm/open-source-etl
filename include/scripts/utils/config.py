import logging
from pydantic import BaseSettings
from airflow import settings as airflow_settings
from airflow.models import Connection
from pathlib import Path
import configparser as cp
from include.scripts import APP_PATH


class Settings(BaseSettings):
    snowflake_username: str
    snowflake_password: str
    snowflake_host: str
    snowflake_db: str
    snowflake_warehouse: str
    snowflake_role: str
    snowflake_schema: str
    snowflake_airflow_conn_id: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_s3_airflow_conn_id: str

    class Config:
        env_file = "dev.env"


class Config:
    def __init__(self):
        self.generic_path = Path(__file__).parent / "../settings.txt"
        self.generic_config = cp.ConfigParser()
        self.generic_config.read(self.generic_path)
        self.entity_list = self.generic_config.get('entities', 'entity_names').split(',')
        self.base_path_raw = self.generic_config.get(
            'file-locations-base', 'inbound_incremental_raw')
        self.base_path_bad = self.generic_config.get(
            'file-locations-base', 'inbound_incremental_bad')
        self.dbt_manifest_path = self.generic_config.get(
            'dbt-settings', 'dbt_manifest_path')
        self.s3_bucket_name = self.generic_config.get(
            's3-details', 's3_bucket_name')
        self.file_delimiter = self.generic_config.get(
            's3-details', 'file_delimiter')
        self.snowflake_stage = self.generic_config.get(
            'snowflake-details', 'snowflake_stage')
        self.snowflake_file_format = self.generic_config.get(
            'snowflake-details', 'snowflake_file_format')
        self.snowflake_raw_schema = self.generic_config.get(
            'snowflake-details', 'snowflake_raw_schema')
        self.snowflake_metadata_schema = self.generic_config.get(
            'snowflake-details', 'snowflake_metadata_schema')
        self.snowflake_error_schema = self.generic_config.get(
            'snowflake-details', 'snowflake_error_schema')
        self.snowflake_stage_schema = self.generic_config.get(
            'snowflake-details', 'snowflake_stage_schema')
        self.snowflake_db_name = self.generic_config.get(
            'snowflake-details', 'snowflake_db_name')
        self.snowflake_wh_name = self.generic_config.get(
            'snowflake-details', 'snowflake_wh_name')
        self.snowflake_role = self.generic_config.get(
            'snowflake-details', 'snowflake_role')
        self.snowflake_metadata_table_name = self.generic_config.get(
            'snowflake-details', 'snowflake_metadata_table_name')
        self.dbt_dir_name = self.generic_config.get(
            'dbt-settings', 'dbt_dir_name')
        self.dbt_dir_full_path = "/".join([APP_PATH, self.dbt_dir_name])
        self.dbt_manifest_full_path = "/".join(
            [APP_PATH, self.dbt_dir_name, self.dbt_manifest_path])
        self.global_cli_flags = self.generic_config.get(
            'dbt-settings', 'global_cli_flags')
        # self.snowflake_account = self.generic_config.get('snowflake-details', 'snowflake_account')
        self.ge_snowflake_datasource = self.generic_config.get(
            'ge-settings', 'ge_snowflake_datasource')
        self.file_partition_levels = self.generic_config.get(
            'file-partition-strategy', 'file_partition_levels')
        self.context_root_dir = self.generic_config.get(
            'ge-settings', 'context_root_dir')
        self.generate_airflow_connnections()

    def get_file_partition_level(self, level):
        file_partition_level = "_".join(['file_partition_level', level])
        return self.generic_config.get(
            'file-partition-strategy', file_partition_level)

    def generate_airflow_connnections(self):
        aws_conn = Connection(
            conn_id=settings.aws_s3_airflow_conn_id,
            conn_type="s3",
            extra={"aws_access_key_id": settings.aws_access_key_id,
                   "aws_secret_access_key": settings.aws_secret_access_key}
        )
        snowflake_conn = Connection(
            conn_id=settings.snowflake_airflow_conn_id,
            conn_type="snowflake",
            host=f"{settings.snowflake_host}.snowflakecomputing.com",
            login=settings.snowflake_username,
            password=settings.snowflake_password,
            extra={"extra__snowflake__account": settings.snowflake_host,
                   "extra__snowflake__aws_access_key_id": settings.aws_access_key_id,
                   "extra__snowflake__aws_secret_access_key": settings.aws_secret_access_key,
                   "extra__snowflake__database": settings.snowflake_db,
                   "extra__snowflake__role": settings.snowflake_role,
                   "extra__snowflake__warehouse": settings.snowflake_warehouse}
        )
        session = airflow_settings.Session()
        aws_conn_name = session.query(Connection).filter(
            Connection.conn_id == aws_conn.conn_id).first()
        sf_conn_name = session.query(Connection).filter(
            Connection.conn_id == snowflake_conn.conn_id).first()
        # Check and create AWS S3 connection
        if str(aws_conn_name) != str(aws_conn.conn_id):
            session.add(aws_conn)
            session.commit()
            logging.info(Connection.log_info(aws_conn))
            logging.info(f'Connection {aws_conn.conn_id} is created')
        else:
            logging.warning(f"Connection {aws_conn.conn_id} already exists")
        # Check and create Snowflake connection
        if str(sf_conn_name) != str(snowflake_conn.conn_id):
            session.add(snowflake_conn)
            session.commit()
            logging.info(Connection.log_info(snowflake_conn))
            logging.info(f'Connection {snowflake_conn.conn_id} is created')
        else:
            logging.warning(
                f"Connection {snowflake_conn.conn_id} already exists")


settings = Settings()
config = Config()
