import datetime
import pendulum
from airflow import DAG, XComArg
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator, S3ListOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.empty import EmptyOperator
from include.scripts.file_services.partitioner import Partitioner
from include.scripts.utils.config import config, settings
import json
import os
import boto3
import pandas as pd

snowflake_error_table = "".join(['t_', 'bundesliga_match_scores', '_errors'])
metadata_key = 'bundesliga_match_scores'

with DAG(
        dag_id='ingest_bundesliga_match_scores',
        schedule_interval=None,
        start_date=pendulum.datetime(2022, 5, 12, tz="Asia/Kolkata"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=['transactional'],
        concurrency=6,
        max_active_runs=1
) as dag:

    def get_key_type(ti, **kwargs):
        dataset_type_select_sql = "SELECT m_key_type FROM {0}.{1}.{2} WHERE m_key = '{3}' " \
            .format(config.snowflake_db_name,
                    config.snowflake_metadata_schema, config.snowflake_metadata_table_name, kwargs['metadata_key'])
        hook_connection = SnowflakeHook(snowflake_conn_id=settings.snowflake_airflow_conn_id)
        sf_conn = hook_connection.get_conn()
        cur = sf_conn.cursor()
        cur.execute(dataset_type_select_sql)
        df = cur.fetch_pandas_all()
        s3_dataset_type = df.values[0][0]
        ti.xcom_push(key='s3_dataset_type', value=s3_dataset_type)

    get_key_type_from_snowflake = PythonOperator(
        task_id="get_key_type_from_snowflake",
        provide_context=True,
        python_callable=get_key_type,
        op_kwargs={'metadata_key': metadata_key}
    )

    file_sniffer_task = S3KeySensor(
        task_id='look_for_bundesliga_match_scores_file_in_s3',
        bucket_name=config.s3_bucket_name,
        bucket_key="/".join([config.base_path_raw,
                             "{{ ti.xcom_pull(task_ids=['get_key_type_from_snowflake'],key='s3_dataset_type')[0] }}",
                             'bundesliga_match_scores', 'bundesliga_match_scores']),
        wildcard_match=True,
        aws_conn_id=settings.aws_s3_airflow_conn_id,
        soft_fail=True,
        timeout=180,
        poke_interval=30,
        mode='reschedule',
        retries=2
    )

    def generate_s3_partition_metadata(ti, **kwargs):
        part = Partitioner()
        file_destination_suffix = part.generate_s3_partition_suffix(kwargs.get('ds'))
        ti.xcom_push(key='file_destination_suffix', value=file_destination_suffix)

    generate_s3_partition = PythonOperator(
        task_id="generate_s3_partition",
        provide_context=True,
        python_callable=generate_s3_partition_metadata
    )

    def generate_metadata_updt_sql(ti, **kwargs):
        s3_suffix = ti.xcom_pull(task_ids=['generate_s3_partition'], key='file_destination_suffix')[0]
        metadata_update_sql = "UPDATE {0}.{1}.{2} SET m_value = m_key_type||'/'||'{4}'||'{3}' WHERE m_key = '{4}' " \
            .format(config.snowflake_db_name, config.snowflake_metadata_schema, config.snowflake_metadata_table_name,
                    s3_suffix, kwargs['metadata_key'])
        ti.xcom_push(key='metadata_update_sql', value=metadata_update_sql)

    generate_metadata_update_sql = PythonOperator(
        task_id='generate_metadata_update_sql',
        provide_context=True,
        python_callable=generate_metadata_updt_sql,
        op_kwargs={'metadata_key': metadata_key}
    )

    update_s3_partition_metadata = SnowflakeOperator(
        task_id='update_bundesliga_match_scores_s3_partition_metadata',
        sql="{{ ti.xcom_pull(task_ids=['generate_metadata_update_sql'], key='metadata_update_sql')[0] }}",
        warehouse=config.snowflake_wh_name,
        database=config.snowflake_db_name,
        schema=config.snowflake_raw_schema,
        role=config.snowflake_role,
        snowflake_conn_id=settings.snowflake_airflow_conn_id
    )

    def get_s3_partition_metadata_helper(ti, **kwargs):
        metadata_select_sql = "SELECT m_value FROM {0}.{1}.{2} WHERE m_key = '{3}' " \
            .format(config.snowflake_db_name, config.snowflake_metadata_schema, config.snowflake_metadata_table_name,
                    kwargs['metadata_key'])
        hook_connection = SnowflakeHook(snowflake_conn_id=settings.snowflake_airflow_conn_id)
        sf_conn = hook_connection.get_conn()
        cur = sf_conn.cursor()
        cur.execute(metadata_select_sql)
        df = cur.fetch_pandas_all()
        s3_partition_suffix = df.values[0][0]
        ti.xcom_push(key='file_destination', value=s3_partition_suffix)

    get_s3_partition_metadata = PythonOperator(
        task_id="get_s3_partition_metadata",
        python_callable=get_s3_partition_metadata_helper,
        op_kwargs={'metadata_key': metadata_key}
    )

    list_of_S3_files_to_copy = S3ListOperator(
        task_id="list_of_S3_files_to_copy",
        bucket=config.s3_bucket_name,
        prefix="/".join([config.base_path_raw, "{{ ti.xcom_pull(task_ids=['get_key_type_from_snowflake'],"
                                               "key='s3_dataset_type')[0] }}",
                         'bundesliga_match_scores', 'bundesliga_match_scores'])
    )

    def copy_files(ti, **kwargs):
        list_of_files_to_copy = ti.xcom_pull(task_ids=['list_of_S3_files_to_copy'], key='return_value')[0]
        s3_partition_suffix = ti.xcom_pull(task_ids=['get_s3_partition_metadata'], key='file_destination')[0]
        s3_resource = boto3.resource('s3')
        for f in range(len(list_of_files_to_copy)):
            file_to_copy = list_of_files_to_copy[f]
            destination = "".join([config.base_path_raw, '/', s3_partition_suffix, file_to_copy.split('/')[-1]])
            origin = "/".join([config.s3_bucket_name, file_to_copy])
            s3_resource.Object(config.s3_bucket_name, destination).copy_from(CopySource=origin)

    copy_files_within_s3 = PythonOperator(
        task_id="copy_files_within_s3",
        python_callable=copy_files
    )

    @task
    def generate_sql(s3_file_path):
        s3_key = s3_file_path[0]
        s3_file = s3_key.split('/')[-1].split('_')[0]
        table_name = "".join(['t_', metadata_key, '_external'])

        try:
            column_mapping_select_sql = " select distinct snowflake_table_column_name " \
                                       "from {0}.{1}.{2} where s3_file_name_wc like '{3}%' " \
                                       "and snowflake_table_name = '{4}' ".format(config.snowflake_db_name,
                                                                                  config.snowflake_metadata_schema,
                                                                                  config.snowflake_colmap_table_name,
                                                                                  s3_file, table_name)
            hook_connection = SnowflakeHook(snowflake_conn_id=settings.snowflake_airflow_conn_id)
            sf_conn = hook_connection.get_conn()
            cur = sf_conn.cursor()
            cur.execute(column_mapping_select_sql)
            df = cur.fetch_pandas_all()
            listOfColms = df['SNOWFLAKE_TABLE_COLUMN_NAME'].tolist()
        except AttributeError:
            s3_client = boto3.client('s3', aws_access_key_id=settings.aws_access_key_id,
                                     aws_secret_access_key=settings.aws_secret_access_key)
            obj = s3_client.get_object(Bucket=config.s3_bucket_name, Key=s3_key)
            data_iter = pd.read_csv(obj['Body'], chunksize=1, sep=config.file_delimiter)
            df = pd.DataFrame(data_iter.get_chunk())
            df.rename(columns={'Unnamed: 0': 'data_id'}, inplace=True)
            # This is only for the specific dataset I have used
            listOfColms = df.columns.values.tolist()

        sql = "create or replace external table {0}.{1}".format(config.snowflake_stage_schema, table_name)
        for i in range(len(listOfColms)):
            if i == 0:
                sql = "".join([sql, '('])
                sql = "".join([sql, listOfColms[i], ' varchar(1000) as (value:c{0}::varchar)'.format(i+1)])
            elif i == len(listOfColms) - 1:
                sql = "".join([sql, ',', listOfColms[i], ' varchar(1000) as (value:c{0}::varchar)'.format(i+1)])
                sql = "".join([sql, ')'])
            else:
                sql = "".join([sql, ',', listOfColms[i], ' varchar(1000) as (value:c{0}::varchar)'.format(i+1)])
        location_string = "".join(['with location = ', '@', config.snowflake_stage_schema,
                                   '.external_table_stage_bundesliga_match_scores'])
        file_format_string = "".join(['file_format =(FORMAT_NAME = ', config.snowflake_stage_schema,
                                      '.external_table_ff)'])
        sql = "".join([sql, ' ', location_string, ' ', file_format_string])
        return sql

    ext_table_sql = generate_sql(s3_file_path=XComArg(list_of_S3_files_to_copy))

    create_external_table = SnowflakeOperator(
        task_id='create_bundesliga_match_scores_external_table',
        sql=ext_table_sql,
        warehouse=config.snowflake_wh_name,
        database=config.snowflake_db_name,
        schema=config.snowflake_stage_schema,
        role=config.snowflake_role,
        snowflake_conn_id=settings.snowflake_airflow_conn_id
    )

    get_key_type_from_snowflake >> file_sniffer_task >> generate_s3_partition >> generate_metadata_update_sql >> \
    update_s3_partition_metadata >> get_s3_partition_metadata >> list_of_S3_files_to_copy >> copy_files_within_s3 >> \
    create_external_table



