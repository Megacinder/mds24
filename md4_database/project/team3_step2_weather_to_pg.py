# aws s3 cp team3_step2_weather_to_pg.py s3://gsb2024airflow/team3_step2_weather_to_pg.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net


from airflow.decorators import dag, task
from airflow.models.xcom_arg import XComArg
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import datetime
from gzip import open as gzip_open
from typing import Generator, Any, List


logger = LoggingMixin().log


ROOT_PATH = "ahremenko_ma"
DAG_PREFIX = "ahremenko_ma"
SOURCE_BUCKET = "db01-content"
CSV_GZ_FILE_DICT = {
    "KFLG": f"{ROOT_PATH}/KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KFSM": f"{ROOT_PATH}/KFSM.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KNYL": f"{ROOT_PATH}/KNYL.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KXNA": f"{ROOT_PATH}/KXNA.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
}
TARGET_CSV_BUCKET = "gsbdwhdata"
PG_CONN_ID = "con_dwh_2024_s004"
PG_TARGET_TABLE = "ods.weather"
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
    :param start_line: exclude some lines as they are not needed
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


def get_lines_from_gzip(gz_filename: str, icao_code: str, start_line: int = 6, stop_line: int = None) -> list:
    """
    get all rows from csv.gz as a list
    :param gz_filename: filename
    :param icao_code: code for insert to table in PG
    :param start_line: exclude some lines before as they are not needed
    :param stop_line: exclude some lines after if they are not needed
    :return: list of lines
    """
    o_list = []
    with gzip_open(gz_filename, 'r') as gzfile:
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
    if stop_line and start_line > stop_line:
        stop_line = None
    o_list = o_list[start_line:stop_line]
    return o_list


def get_table_dml_and_rows_for_insert(lines: List[list]) -> tuple:
    """
    gives table dml and rows for insert to table in PG
    :param lines: lines of CSV file as list of lists
    :return: table dml and rows list
    """
    headers = lines[0]
    headers = ["icao_code", "local_time"] + [f"raw_{col_name.lower()}" for col_name in headers[2:]]

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

        cur.execute(SQL_IS_PG_TABLE_EXISTS)
        is_table_exists = cur.fetchone()[0]
        logger.info("Is table exists: " + str(is_table_exists))

        if is_table_exists:
            sql_truncate_table = f"truncate table {PG_TARGET_TABLE}"
            logger.info(sql_truncate_table)
            cur.execute(sql_truncate_table)
            conn.commit()

        for icao_code, csv_gz_file in CSV_GZ_FILE_DICT.items():
            file_path = s3_hook.download_file(
                key=csv_gz_file,
                bucket_name=TARGET_CSV_BUCKET,
            )

            lines = get_lines_from_gzip(file_path, icao_code)
            rows, create_table_sql = get_table_dml_and_rows_for_insert(lines)
            cur.execute(create_table_sql)
            conn.commit()

            logger.info("If table not exists then " + create_table_sql)

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


    create_table_and_insert_rows_from_gzip()


dag = dag1()


if __name__ == "__main__":
    from dotenv import load_dotenv
    from os import getenv
    load_dotenv()

    DB_AIRPORT_WEATHER_PATH = getenv("DB_AIRPORT_WEATHER_PATH")
    CSV_GZ_FILE = f"{DB_AIRPORT_WEATHER_PATH}/KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz"
    ICAO_CODE = "KFLG"

    # gen = get_line_from_gzip_generator(CSV_GZ_FILE)
    # print(next(gen))
    # print(next(gen))
    # print(next(gen))

    # rows1 = get_lines_from_gzip(CSV_GZ_FILE)
    lines1 = get_lines_from_gzip(CSV_GZ_FILE, ICAO_CODE, stop_line=9)
    rows1, dml1 = get_table_dml_and_rows_for_insert(lines1)
    for row1 in rows1:
        print(row1)
