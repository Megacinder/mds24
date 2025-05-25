# aws s3 cp team3_step2_weather_to_pg.py s3://gsb2024airflow/team3_step2_weather_to_pg.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net


from airflow.decorators import dag, task
from airflow.models.xcom_arg import XComArg
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.empty import EmptyOperator

from datetime import datetime
from gzip import open as gzip_open
from typing import Generator, Any, List


logger = LoggingMixin().log


ROOT_PATH = "ahremenko_ma"
DAG_PREFIX = "ahremenko_ma"
SOURCE_BUCKET = "db01-content"

CSV_GZ_FILE_LIST = {
    "KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KFSM.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KNYL.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KXNA.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
}
TARGET_CSV_BUCKET = "gsbdwhdata"
PG_CONN_ID = "con_dwh_2024_s004"
AWS_CONN_ID = "object_storage_yc"
PG_TARGET_TABLE = "ods.weather"


CSV_GZ_FILE_DICT = {
    i[:4]: f"{ROOT_PATH}/{i}"
    for i
    in CSV_GZ_FILE_LIST
}


DAG_PARAM = dict(
    dag_id=f"{DAG_PREFIX}_weather_airport",
    schedule=None,
    start_date=datetime(2025, 5, 1),
    catchup=False,
    description=f"{DAG_PREFIX}_weather_airport",
    tags=[f"{DAG_PREFIX}"],
)


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


def get_table_ddl(lines: List[list]) -> str:
    """
    gives table ddl
    :param lines: lines of CSV file as list of lists
    :return: table ddl
    """
    headers = lines[0]
    headers = ["icao_code", "local_time"] + [f"raw_{col_name.lower()}" for col_name in headers[2:]]

    columns = [f'"{header}" TEXT' for header in headers]
    create_table_sql = f"""
        create table if not exists {PG_TARGET_TABLE} (
            {', '.join(columns)}
        );
    """

    return create_table_sql


s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)


@dag(**DAG_PARAM)
def dag1() -> None:
    """
    the dev team 3 dag to load airports' weather data
    """

    start = EmptyOperator(task_id="start")
    stop = EmptyOperator(task_id="stop")


    @task.python(do_xcom_push=False)
    def create_table_or_truncate() -> (XComArg | None):
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        header_lines = get_lines_from_gzip(file_path, icao_code, stop_line=8)
        create_table_sql = get_table_ddl(header_lines)
        logger.info(create_table_sql)
        cur.execute(create_table_sql)

        sql_truncate_table = f"truncate table {PG_TARGET_TABLE}"
        logger.info(sql_truncate_table)
        cur.execute(sql_truncate_table)
        conn.commit()


    tasks = []

    for icao_code, csv_gz_file in CSV_GZ_FILE_DICT.items():
        file_path = s3_hook.download_file(
            key=csv_gz_file,
            bucket_name=TARGET_CSV_BUCKET,
        )
        def create_task(icao_code: str, file_path: str) -> (XComArg | None):
            @task.python(do_xcom_push=False, task_id=f"load_to_db_airport_{icao_code}")
            def insert_rows_from_gzip() -> (XComArg | None):
                """
                creates a table for airport weather data and loads the data there
                :return: nothing, but actually airflow's XComArg
                """
                conn = postgres_hook.get_conn()
                cur = conn.cursor()


                rows = get_lines_from_gzip(file_path, icao_code, start_line=7)

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

            return insert_rows_from_gzip()

        task1 = create_task(icao_code, file_path)
        tasks.append(task1)


    start >> create_table_or_truncate() >> tasks >> stop


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
    print(CSV_GZ_FILE_DICT)

    # rows1 = get_lines_from_gzip(CSV_GZ_FILE)
    # lines1 = get_lines_from_gzip(CSV_GZ_FILE, ICAO_CODE, stop_line=9)
    # rows1, dml1 = get_table_dml_and_rows_for_insert(lines1)
    # for row1 in lines1:
    #     print(row1)
