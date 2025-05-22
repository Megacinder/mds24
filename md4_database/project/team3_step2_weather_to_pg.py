# aws s3 cp team3_step2_weather_to_pg.py s3://gsb2024airflow/team3_step2_weather_to_pg.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net


from airflow.decorators import dag, task
from airflow.models.xcom_arg import XComArg
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
from typing import Generator, Any
from gzip import open as gzip_open



logger = LoggingMixin().log


ROOT_PATH = "ahremenko_ma"
DAG_PREFIX = "ahremenko_ma"
SOURCE_BUCKET = "db01-content"
CSV_GZ_FILE_LIST = [
    f"{ROOT_PATH}/KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    f"{ROOT_PATH}/KFSM.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    f"{ROOT_PATH}/KNYL.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    f"{ROOT_PATH}/KXNA.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
]
TARGET_CSV_BUCKET = "gsbdwhdata"
PG_CONN_ID = "con_dwh_2024_s004"
PG_TARGET_TABLE = "ods.weather"

DAG_PARAM = dict(
    dag_id=f"{DAG_PREFIX}_weather_airport",
    schedule=None,
    start_date=datetime(2025, 5, 1),
    catchup=False,
    description=f"{DAG_PREFIX}_weather_airport",
    tags=[f"{DAG_PREFIX}"],
)

s3_hook = S3Hook(aws_conn_id="object_storage_yc")


def get_line_from_gzip_generator(gz_filename: str, start_line: int = 7) -> Generator[list, Any, None]:
    """
    get row from csv.gz
    :param gz_filename:
    :param start_line:
    :return: row from the file as a Generator[list, Any, None]
    """
    with gzip_open(gz_filename, 'r') as gzfile:
        for _ in range(start_line - 1):
            next(gzfile)

        for line in gzfile:
            decoded_line = (
                line
                .decode("utf-8")
                .strip()
                .replace('"', '')
            )[:-1]
            yield decoded_line.split(';')


def get_lines_from_gzip(gz_filename: str, start_line: int = 6) -> list:
    from pathlib import Path
    o_list = []
    with gzip_open(gz_filename, 'r') as gzfile:
        filename = Path(gz_filename).name
        icao_code = filename[:4]
        for line in gzfile:
            decoded_line = (
                line
                .decode("utf-8")
                .strip()
                .replace('"', '')
                .replace("'", '_')
            )[:-1]
            o_list.append([icao_code] + decoded_line.split(';'))
    # o_list = list(set(o_list))  # deduplication
    o_list = o_list[start_line:]
    return o_list


def get_table_dml_and_rows_for_insert(file_path: str) -> tuple:
    lines = get_lines_from_gzip(file_path)
    headers = lines[0]
    headers = ["icao_code", "local_time"] + headers[2:]

    columns = [f'"{header}" TEXT' for header in headers]
    create_table_sql = f"""
        create table if not exists {PG_TARGET_TABLE} (
            {', '.join(columns)}
        );
    """
    lines = lines[1:]

    return lines, create_table_sql


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
    def create_table_and_insert_rows_from_gzip() -> (XComArg | None):
        """
        creates a table for airport weather data and loads the data there
        :return: nothing, but actually airflow's XComArg
        """
        postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        sql_is_table_exists = f"""
        select
            exists(
                select
                from
                    pg_tables
                where 1=1
                    and schemaname || '.' || tablename = '{PG_TARGET_TABLE}'
            )
        """
        cur.execute(sql_is_table_exists)

        print(cur.fetchone())

        if cur.fetchone():
            sql_truncate_table = f"truncate table {PG_TARGET_TABLE}"
            cur.execute(sql_truncate_table)


        for csv_gz_file in CSV_GZ_FILE_LIST:
            file_path = s3_hook.download_file(
                key=csv_gz_file,
                bucket_name=TARGET_CSV_BUCKET,
            )

            rows, create_table_sql = get_table_dml_and_rows_for_insert(file_path)
            logger.info(create_table_sql)

            cur.execute(create_table_sql)

            logger.info(rows[0])
            for row in rows:
                sql_insert_row = f"""
                    insert into {PG_TARGET_TABLE}
                    values (
                        {"'" + "', '".join(row) + "'"}
                    )
                """
                cur.execute(sql_insert_row)
            logger.info(rows[-1])

            conn.commit()
            logger.info("Table created successfully")


    create_table_and_insert_rows_from_gzip()


dag = dag1()


if __name__ == "__main__":
    from dotenv import load_dotenv
    from os import getenv
    load_dotenv()

    DB_AIRPORT_WEATHER_PATH = getenv("DB_AIRPORT_WEATHER_PATH")
    CSV_GZ_FILE = f"{DB_AIRPORT_WEATHER_PATH}/KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz"

    # gen = get_line_from_gzip_generator(CSV_GZ_FILE)
    # print(next(gen))
    # print(next(gen))
    # print(next(gen))

    # rows1 = get_lines_from_gzip(CSV_GZ_FILE)
    rows1, dml = get_table_dml_and_rows_for_insert(CSV_GZ_FILE)
    print(rows1)
    print(dml)
    for row1 in rows1[:4]:
        print(row1)
