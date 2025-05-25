# aws s3 cp team3_step2_weather_to_pg.py s3://gsb2024airflow/team3_step2_weather_to_pg.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net

SQL_IS_PG_TABLE_EXISTS = f"""
select
    exists(
        select
        from
            pg_tables
        where 1=1
            and schemaname || '.' || tablename = '{PG_TARGET_TABLE}'
    )
"""


from airflow.decorators import dag, task
from airflow.models.xcom_arg import XComArg
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from gzip import open as gzip_open
from typing import Generator, Any, List


CSV_GZ_FILE_LIST = {
    "KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KFSM.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KNYL.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KXNA.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
}
CSV_GZ_FILE_DICT = {i[:4]: f"{ROOT_PATH}/{i}" for i in CSV_GZ_FILE_LIST}

@dag(**DAG_PARAM)
def dag1() -> None:
    start = EmptyOperator(task_id="start")
    stop = EmptyOperator(task_id="stop")

    tasks = []
    for icao_code, csv_gz_filels in CSV_GZ_FILE_DICT.items():
        def create_task(icao_code: str, file_path: str) -> (XComArg | None):
            @task.python(do_xcom_push=False, task_id=f"load_to_db_airport_{icao_code}")
            def insert_rows_from_gzip() -> (XComArg | None):
                ...  # logics to load table in a database
            return insert_rows_from_gzip()

        task1 = create_task(icao_code, file_path)
        tasks.append(task1)

    start >> tasks >> stop

dag = dag1()
