# aws s3 cp team3_step1_download_airport_csv.py s3://gsbdwhdata/ahremenko_ma/team3_step1_download_airport_csv.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net
# aws s3 cp team3_step1_download_airport_csv.py s3://gsb2024airflow/team3_step1_download_airport_csv.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net
# aws s3 cp team3_etl_dag.py s3://gsb2024airflow/team3_etl_dag.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net


from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

from csv import reader as csv_reader, writer as csv_writer
from datetime import datetime
from gzip import open as gzip_open
from logging import getLogger, Filter, WARNING
from os import path, unlink
from tempfile import NamedTemporaryFile
from typing import Generator, Any, List


class SuppressRequestsFilter(Filter):
    """
    cut logs - so that a task doesn't shit in the logs
    """
    def filter(self, record):
        return not record.name.startswith("urllib3")


ROOT_FILE_PATH = "ahremenko_ma"
DAG_TAG = "team_3"

FLIGHT_SOURCE_BUCKET = "db01-content"
FLIGHT_SOURCE_PATH = "flights"
FLIGHT_FILE_NAME_TEMPLATE = "T_ONTIME_REPORTING-"

AIRPORT_CSV_URL = "https://ourairports.com/data/airports.csv"
AIRPORT_CSV_PATH = f"{ROOT_FILE_PATH}/airports.csv"
TARGET_CSV_BUCKET = "gsbdwhdata"

AWS_CONN_ID = "object_storage_yc"
PG_CONN_ID = "con_dwh_2024_s004"
PG_DB_NAME = "dwh_2024_s004"

ODS_WEATHER_TABLE = "ods.weather"
ODS_AIRPORT_TABLE = "ods.airport"
ODS_FLIGHT_TABLE = "ods.flight"

WEATHER_CSV_GZ_FILE_LIST = {
    "KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KFSM.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KNYL.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KXNA.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
}
WEATHER_CSV_GZ_FILE_DICT = {
    i[:4]: f"{ROOT_FILE_PATH}/{i}"
    for i
    in WEATHER_CSV_GZ_FILE_LIST
}

DAG_PARAM = dict(
    dag_id=f"ahremenko_ma_{DAG_TAG}_etl_test",
    schedule=None,
    start_date=datetime(2025, 5, 1),
    catchup=False,
    description=f"{DAG_TAG}_etl_dag",
    tags=[f"{DAG_TAG}"],
    max_active_tasks=5,
)


is_downloading_needed = Variable.get("ahremenko_ma_is_downloading_needed", default_var=False)
if is_downloading_needed in ('True', 'False', '1', '0'):
    is_downloading_needed = bool(is_downloading_needed)
else:
    is_downloading_needed = False


logger = LoggingMixin().log

s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)


def get_flight_file_list(s3_hook: S3Hook, bucket_name: str, prefix: str, file_name_template: str) -> list:
    all_files = s3_hook.list_keys(
        bucket_name=bucket_name,
        prefix=f"{prefix}/"
    )
    logger.info(f"all_files {all_files}")
    logger.info(f"start with {bucket_name}/{file_name_template}")
    matching_files = [
        file for file in all_files
        if file.startswith(f"{bucket_name}/{file_name_template}") and file.endswith('.csv')
    ]
    logger.info(f"matching_files {matching_files}")
    return matching_files


@dag(**DAG_PARAM)
def dag1() -> None:
    """
    the dev team 3 etl
    """
    start = EmptyOperator(task_id="start")
    stop = EmptyOperator(task_id="stop")

    @task(do_xcom_push=False)
    def copy_all_flight_files() -> (XComArg | None | list):
        target_file_name_list = []
        for file_name in get_flight_file_list(
            s3_hook=s3_hook,
            bucket_name=TARGET_CSV_BUCKET,
            prefix=FLIGHT_SOURCE_PATH,
            file_name_template=FLIGHT_FILE_NAME_TEMPLATE,
        ):
            logger.info(f"copying {file_name}")
            file_id = file_name.split(FLIGHT_FILE_NAME_TEMPLATE)[1].replace('.csv', '')
            destination_key = f"{ROOT_FILE_PATH}/flight_{file_id}.csv"

            file_data = s3_hook.read_key(file_name, FLIGHT_SOURCE_BUCKET)
            logger.info("Downloading file %s to %s", file_id, destination_key)
            s3_hook.load_string(
                string_data=file_data,
                key=destination_key,
                bucket_name=TARGET_CSV_BUCKET,
                replace=True
            )
            target_file_name_list.append(f"{TARGET_CSV_BUCKET}/{destination_key}")

        return target_file_name_list



    # def write_file_to_db() -> None:
    #     s3_hook = S3Hook(aws_conn_id='object_storage_yc')
    #     file_name = s3_hook.download_file(
    #         key=S3_LOCATION,
    #         bucket_name=S3_BUCKET,
    #     )
    #
    #     if file_name:
    #         postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    #         conn = postgres_hook.get_conn()
    #         cur = conn.cursor()
    #         with open(file_name, "r") as file:
    #             cur.copy_expert(
    #                 "COPY ods.flights FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
    #                 file,
    #             )
    #         conn.commit()


    for csv_file in get_flight_file_list(
        s3_hook,
        TARGET_CSV_BUCKET,
        ROOT_FILE_PATH,
        FLIGHT_FILE_NAME_TEMPLATE,
    ):
        logger.info(csv_file)
        # file_path = s3_hook.download_file(
        #     key=csv_gz_file,
        #     bucket_name=TARGET_CSV_BUCKET,
        # )
        # def create_load_to_ods_flight_data_task(icao_code: str, file_path: str) -> (XComArg | None):
        #     @task.python(do_xcom_push=True, task_id=f"load_to_ods_airport_data_for_{icao_code}")
        #     def insert_rows_from_csv() -> (XComArg | None):
        #         """
        #         insert weather data to a database's table in parallel - from 4 files simultaneously
        #         :return: nothing, but actually airflow's XComArg
        #         """
        #         rows = get_lines_from_gzip(file_path, icao_code, start_line=7)
        #
        #
        #
        #         with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn().cursor() as cur:
        #             with open(tmp_path, 'r') as file:
        #                 cur.execute("SELECT current_database(), current_schema()")
        #                 db, schema = cur.fetchone()
        #                 logger.info(f"Current DB: {db}, Current schema: {schema}")
        #                 sql_pg_copy_csv = f"copy {ODS_WEATHER_TABLE} from stdin with csv delimiter as ',' quote '\"'"
        #                 cur.copy_expert(sql_pg_copy_csv, file)
        #
        #         if tmp_path and path.exists(tmp_path):
        #             unlink(tmp_path)
        #
        #     return insert_rows_from_gzip()
        #
        # task1 = create_load_to_ods_airport_data_task(icao_code, file_path)
        # load_airport_csv_gz_to_db_tasks.append(task1)

    start >> copy_all_flight_files() >> stop

dag = dag1()
