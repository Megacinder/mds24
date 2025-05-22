# aws s3 cp team3_step1_download_airport_csv.py s3://gsbdwhdata/ahremenko_ma/team3_step1_download_airport_csv.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net
# aws s3 cp team3_step1_download_airport_csv.py s3://gsb2024airflow/team3_step1_download_airport_csv.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.xcom_arg import XComArg
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import csv
import logging


IS_DOWNLOADING_NEEDED = Variable.get("ahremenko_ma_is_downloading_needed")
if IS_DOWNLOADING_NEEDED in ('True', 'False', '1', '0'):
    is_downloading_needed = bool(IS_DOWNLOADING_NEEDED)
else:
    is_downloading_needed = False


class SuppressRequestsFilter(logging.Filter):
    """
    cut logs - so that a task doesn't shit in the logs
    """
    def filter(self, record):
        return not record.name.startswith("urllib3")


logger = LoggingMixin().log


ROOT_PATH = "ahremenko_ma"
DAG_PREFIX = "ahremenko_ma"
SOURCE_BUCKET = "db01-content"
CSV_URL = "https://ourairports.com/data/airports.csv"
TARGET_CSV_PATH = f"{ROOT_PATH}/airports.csv"
TARGET_CSV_BUCKET = "gsbdwhdata"
PG_CONN_ID = "con_dwh_2024_s004"
PG_TABLE = "ods.airport"

DAG_PARAM = dict(
    dag_id=f"{DAG_PREFIX}_airport",
    schedule=None,
    start_date=datetime(2025, 5, 1),
    catchup=False,
    description=f"{DAG_PREFIX}_airport",
    tags=[f"{DAG_PREFIX}"],
)

s3_hook = S3Hook(aws_conn_id="object_storage_yc")


@dag(**DAG_PARAM)
def dag1() -> None:
    """
    the name of tasks I tried to choose quite clear, but regarding the homework task
    "
        Для каждого процесса Airflow нужно написать комментарий,
        для чего сделан поток и что он делает (без chat gpt).
    "
    the descriptions is there
    """

    @task.python(do_xcom_push=False)
    def download_airport_csv_file() -> (XComArg | None):
        """
        downloads airport csv file
        :return: nothing, but actually airflow's XComArg
        MA: maybe we can try to filter it and load to DB via Pandas w/o loading to s3?..
        """
        import requests
        import certifi
        logger.info(certifi.where())

        logger_task = logging.getLogger("airflow.task")
        logger_task.setLevel(logging.WARNING)
        logger_task.addFilter(SuppressRequestsFilter())

        logger.info(type(IS_DOWNLOADING_NEEDED))
        logger.info(bool(IS_DOWNLOADING_NEEDED))

        if is_downloading_needed:
            logger.info("Start downloading airport csv file")
            response = requests.get(CSV_URL, verify=False)
            response.raise_for_status()

            s3_hook.load_string(
                string_data=response.text,
                key=TARGET_CSV_PATH,
                bucket_name=TARGET_CSV_BUCKET,
                replace=True,
            )
            logger.info("File downloaded successfully")
        else:
            logger.info("We don't need to download airport csv file")


    @task.python(do_xcom_push=False)
    def create_table() -> (XComArg | None):
        """
        creates a table for airport data from the previous task
        :return: nothing, but actually airflow's XComArg
        """
        file_path = s3_hook.download_file(
            key=TARGET_CSV_PATH,
            bucket_name=TARGET_CSV_BUCKET,
        )

        with open(file_path, "r", encoding="utf-8") as file:
            csv_reader = csv.reader(file)
            headers = next(csv_reader)


        columns = [f'"{header}" TEXT' for header in headers]
        sql_create_table = f"""
            create table if not exists {PG_TABLE} (
                {', '.join(columns)}
            );
        """

        postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql_create_table)
        conn.commit()

        logger.info("Table created successfully")


    @task.python(do_xcom_push=False)
    def write_file_to_db() -> (XComArg | None):
        """
        write the airport data to the table from the tasks
        :return: nothing, but actually airflow's XComArg
        """

        file_path = s3_hook.download_file(
            key=TARGET_CSV_PATH,
            bucket_name=TARGET_CSV_BUCKET,
        )

        if file_path:
            postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
            conn = postgres_hook.get_conn()
            cur = conn.cursor()

            sql_truncate_table = f"truncate table {PG_TABLE}"
            cur.execute(sql_truncate_table)

            with open(file_path, "r") as file:
                sql_pg_copy_csv = f"copy {PG_TABLE} from stdin with csv header delimiter as ',' quote '\"'"
                cur.copy_expert(sql_pg_copy_csv, file)

            conn.commit()


    download_airport_csv_file() >> create_table() >> write_file_to_db()


dag = dag1()
