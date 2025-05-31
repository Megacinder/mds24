# aws s3 cp team3_step1_download_airport_csv.py s3://gsbdwhdata/ahremenko_ma/team3_step1_download_airport_csv.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net
# aws s3 cp team3_step1_download_airport_csv.py s3://gsb2024airflow/team3_step1_download_airport_csv.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net
# aws s3 cp team3_etl_dag123.py s3://gsb2024airflow/team3_etl_dag123.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net

class Metadata:
    version = 4
    type = 'csv load into ods and dds only'


from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

from csv import writer as csv_writer
from datetime import datetime
from gzip import open as gzip_open
from logging import getLogger, Filter, WARNING
from os import path, unlink, environ
from tempfile import NamedTemporaryFile



class SuppressRequestsFilter(Filter):
    """
    cut logs - so that a task doesn't shit in the logs
    """
    def filter(self, record):
        return not record.name.startswith("urllib3")


ETL_PARAM = dict(
    root_file_path="ahremenko_ma",  # airport and flight data in my - ahremenko_ma - path only
    dag_id="ahremenko_ma_team_3_etl_dag_csv",
    dag_tag="team_3",
    pg_conn_id="con_dwh_2024_s004",
    pg_db_name="dwh_2024_s004",
)

AIRPORT_CSV_PATH = f"{ETL_PARAM["root_file_path"]}/airports.csv"
AIRPORT_TZ_CSV_PATH = f"{ETL_PARAM["root_file_path"]}/airport_tz.csv"

WEATHER_CSV_GZ_FILE_LIST = {
    "KFLG.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KFSM.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KNYL.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
    "KXNA.01.01.2024.01.01.2025.1.0.0.en.utf8.00000000.csv.gz",
}
FLIGHT_CSV_FILE_LIST_RAW = {
    # "flight_2023-10.csv",
    # "flight_2023-11.csv",
    # "flight_2023-12.csv",
    "flight_2024-01.csv",
    "flight_2024-02.csv",
    "flight_2024-03.csv",
    "flight_2024-04.csv",
    "flight_2024-05.csv",
    "flight_2024-06.csv",
    "flight_2024-07.csv",
}
WEATHER_CSV_GZ_FILE_DICT = {
    i[:4]: f"{ETL_PARAM["root_file_path"]}/{i}"
    for i
    in WEATHER_CSV_GZ_FILE_LIST
}
FLIGHT_CSV_FILE_LIST = [f"{ETL_PARAM["root_file_path"]}/{i}" for i in FLIGHT_CSV_FILE_LIST_RAW]




TARGET_CSV_BUCKET = "gsbdwhdata"
AIRPORT_CSV_URL = "https://ourairports.com/data/airports.csv"
FLIGHT_SOURCE_BUCKET = "db01-content"
FLIGHT_SOURCE_PATH = "flights"
FLIGHT_FILE_NAME_TEMPLATE = "T_ONTIME_REPORTING-"
AWS_CONN_ID = "object_storage_yc"

ODS_WEATHER_TABLE = "ods.weather"
REF_AIRPORT_TABLE = "dds.airport"
REF_AIRPORT_TZ_TABLE = "dds.airport_tz"
ODS_FLIGHT_TABLE = "ods.flight"


DAG_PARAM = dict(
    dag_id=f"{ETL_PARAM["dag_id"]}",
    schedule=None,
    start_date=datetime(2025, 5, 1),
    catchup=False,
    description=f"supadupadag",
    tags=[f"{ETL_PARAM["dag_tag"]}"],
    max_active_tasks=5,
)


logger = LoggingMixin().log


IS_LOCAL = "AIRFLOW_HOME" not in environ and "AIRFLOW__CORE__DAGS_FOLDER" not in environ
if not IS_LOCAL:
    is_downloading_needed = Variable.get("ahremenko_ma_is_downloading_needed", default_var=False)
    if is_downloading_needed in ('True', '1', 'Da', 'Go', 'Huli net?'):
        is_downloading_needed = bool(is_downloading_needed)
    else:
        is_downloading_needed = False


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


@dag(**DAG_PARAM)
def dag1() -> None:
    """
    the dev team 3 etl
    """
    start = EmptyOperator(task_id="start")
    stop = EmptyOperator(task_id="stop")


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

        logger_task = getLogger("airflow.task")
        logger_task.setLevel(WARNING)
        logger_task.addFilter(SuppressRequestsFilter())

        logger.info(is_downloading_needed)


        if is_downloading_needed:
            logger.info("Start downloading airport csv file")
            response = requests.get(AIRPORT_CSV_URL, verify=False)
            response.raise_for_status()

            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            s3_hook.load_string(
                string_data=response.text,
                key=AIRPORT_CSV_PATH,
                bucket_name=TARGET_CSV_BUCKET,
                replace=True,
            )
            logger.info("File downloaded successfully")
        else:
            logger.info("We don't need to download airport csv file")


    @task.python(do_xcom_push=False)
    def create_ods_tables() -> (XComArg | None):
        """
        creates ods schema and tables
        :return: nothing, but actually airflow's XComArg
        """
        sql = """
        create schema if not exists ods
        ;
        
        
        --drop table if exists ods.flight
        --;
        create table if not exists ods.flight (
             year                       text
            ,month                      text
            ,flight_dt                  text
            ,carrier_code               text
            ,tail_num                   text
            ,carrier_flight_num         text
            ,origin_code                text
            ,origin_city_name           text
            ,dest_code                  text
            ,dest_city_name             text
            ,scheduled_dep_tm           text
            ,actual_dep_tm              text
            ,dep_delay_min              text
            ,dep_delay_group_num        text
            ,wheels_off_tm              text
            ,wheels_on_tm               text
            ,scheduled_arr_tm           text
            ,actual_arr_tm              text
            ,arr_delay_min              text
            ,arr_delay_group_num        text
            ,cancelled_flg              text
            ,cancellation_code          text
            ,flights_cnt                text
            ,distance                   text
            ,distance_group_num         text
            ,carrier_delay_min          text
            ,weather_delay_min          text
            ,nas_delay_min              text
            ,security_delay_min         text
            ,late_aircraft_delay_min    text
        )
        ;


        --drop table if exists ods.weather
        --;
        create table if not exists ods.weather (
             icao_code   text
            ,local_time  text
            ,raw_t       text
            ,raw_p0      text
            ,raw_p       text
            ,raw_u       text
            ,raw_dd      text
            ,raw_ff      text
            ,raw_ff10    text
            ,raw_ww      text
            ,raw_w_w_    text
            ,raw_c       text
            ,raw_vv      text
            ,raw_td      text
        )
        ;

        """

        with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            logger.info("ods scheme and tables there were created")

    @task.python(do_xcom_push=False)
    def create_dds_airport_plus_tz_tables() -> (XComArg | None):
        """
        creates dds schema and dds airport tables
        :return: nothing, but actually airflow's XComArg
        """
        sql = """
        create schema if not exists dds;


        --drop table if exists dds.airport
        --;
        create table if not exists dds.airport (
             id                 text
            ,ident              text
            ,type               text
            ,name               text
            ,latitude_deg       text
            ,longitude_deg      text
            ,elevation_ft       text
            ,continent          text
            ,iso_country        text
            ,iso_region         text
            ,municipality       text
            ,scheduled_service  text
            ,icao_code          text
            ,iata_code          text
            ,gps_code           text
            ,local_code         text
            ,home_link          text
            ,wikipedia_link     text
            ,keywords           text
        )
        ;
        create unique index if not exists ix_uniq_dds_airport_iata on dds.airport (iata_code)
        ;


        --drop table if exists dds.airport_tz
        --;
        create table if not exists dds.airport_tz (
            iata_code text
            ,tz       text
            ,constraint dds_airport_tz_pk primary key (iata_code)
        )
        ;
        """

        with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            logger.info("dds airport tables has been created (or exist)")


    @task.python(do_xcom_push=False)
    def write_airport_csv_to_dds_airport() -> (XComArg | None):
        """
        write the airport data to the table from the tasks
        :return: nothing, but actually airflow's XComArg
        """
        logger_task = getLogger("airflow.task")
        logger_task.setLevel(WARNING)
        logger_task.addFilter(SuppressRequestsFilter())

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        file_path = s3_hook.download_file(
            key=AIRPORT_CSV_PATH,
            bucket_name=TARGET_CSV_BUCKET,
        )

        with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
            if file_path:
                with open(file_path, "r") as file:
                    cur = conn.cursor()
                    cur.execute(f"truncate table {REF_AIRPORT_TABLE}")
                    sql_pg_copy_csv = f"copy {REF_AIRPORT_TABLE} from stdin with csv header delimiter as ',' quote '\"'"
                    cur.copy_expert(sql_pg_copy_csv, file)
                    conn.commit()


    @task.python(do_xcom_push=False)
    def write_airport_tz_csv_to_dds_airport() -> (XComArg | None):
        """
        write the airport data to the table from the tasks
        :return: nothing, but actually airflow's XComArg
        """
        logger_task = getLogger("airflow.task")
        logger_task.setLevel(WARNING)
        logger_task.addFilter(SuppressRequestsFilter())

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        file_path = s3_hook.download_file(
            key=AIRPORT_TZ_CSV_PATH,
            bucket_name=TARGET_CSV_BUCKET,
        )

        with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
            if file_path:
                with open(file_path, "r") as file:
                    cur = conn.cursor()
                    cur.execute(f"truncate table {REF_AIRPORT_TZ_TABLE}")
                    sql_pg_copy_csv = f"copy {REF_AIRPORT_TZ_TABLE} from stdin with csv header delimiter as ',' quote '\"'"
                    cur.copy_expert(sql_pg_copy_csv, file)
                    conn.commit()


    @task.python(do_xcom_push=True)
    def copy_from_gzip_to_ods_weather() -> (XComArg | None):
        """
        insert weather data to a database's table  in a chain (not parallel) from 4 files
        :return: nothing, but actually airflow's XComArg
        """
        if IS_LOCAL: return

        for icao_code, csv_gz_file in WEATHER_CSV_GZ_FILE_DICT.items():
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            file_path = s3_hook.download_file(
                key=csv_gz_file,
                bucket_name=TARGET_CSV_BUCKET,
            )

            logger_task = getLogger("airflow.task")
            logger_task.setLevel(WARNING)
            logger_task.addFilter(SuppressRequestsFilter())

            rows = get_lines_from_gzip(file_path, icao_code, start_line=7)

            with NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as tmp:
                csv_writer_ex = csv_writer(tmp)
                csv_writer_ex.writerows(rows)
                tmp_path = tmp.name


            with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
                with open(tmp_path, 'r') as file:
                    cur = conn.cursor()
                    cur.execute(f"truncate table {ODS_WEATHER_TABLE}")
                    sql_pg_copy_csv = f"copy {ODS_WEATHER_TABLE} from stdin with csv delimiter as ',' quote '\"'"
                    cur.copy_expert(sql_pg_copy_csv, file)
                    conn.commit()

            if tmp_path and path.exists(tmp_path):
                unlink(tmp_path)


    @task.python(do_xcom_push=False)
    def copy_all_flight_files() -> (XComArg | None):
        logger_task = getLogger("airflow.task")
        logger_task.setLevel(WARNING)
        logger_task.addFilter(SuppressRequestsFilter())

        logger.info("is_downloading_needed, %s", is_downloading_needed)

        if is_downloading_needed:
            logger.info("Downloading flight data")
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            all_files = s3_hook.list_keys(
                bucket_name=FLIGHT_SOURCE_BUCKET,
                prefix=f"{FLIGHT_SOURCE_PATH}/"
            )

            matching_files = [
                file for file in all_files
                if file.startswith(f"{FLIGHT_SOURCE_PATH}/{FLIGHT_FILE_NAME_TEMPLATE}") and file.endswith('.csv')
            ]

            for source_key in matching_files:
                logger.info("Downloading the file %s", source_key)
                file_id = source_key.split(FLIGHT_FILE_NAME_TEMPLATE)[1].replace('.csv', '')
                destination_key = f"{ETL_PARAM["root_file_path"]}/flight_{file_id}.csv"

                file_data = s3_hook.read_key(source_key, FLIGHT_SOURCE_BUCKET)
                s3_hook.load_string(
                    string_data=file_data,
                    key=destination_key,
                    bucket_name=TARGET_CSV_BUCKET,
                    replace=True
                )
            logger.info("Downloading flight data files completed")
        else:
            log_info = """
                Downloading is not needed because the 'is_downloading_needed' variable
                (Variable.get('ahremenko_ma_is_downloading_needed') is set to False
            """
            logger.info(log_info)


    @task.python(do_xcom_push=True)
    def copy_from_csv_to_ods_flight() -> (XComArg | None):
        """
        insert flight data to a database's table in a chain (not parallel) from several files
        :return: nothing, but actually airflow's XComArg
        """
        if IS_LOCAL:
            return

        for csv_file in FLIGHT_CSV_FILE_LIST:
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            file_path = s3_hook.download_file(
                key=csv_file,
                bucket_name=TARGET_CSV_BUCKET,
            )

            logger_task = getLogger("airflow.task")
            logger_task.setLevel(WARNING)
            logger_task.addFilter(SuppressRequestsFilter())

            with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
                with open(file_path, 'r') as file:
                    cur = conn.cursor()
                    sql_pg_copy_csv = f"copy {ODS_FLIGHT_TABLE} from stdin with csv header delimiter as ',' quote '\"'"
                    cur.copy_expert(sql_pg_copy_csv, file)
                    conn.commit()

    # (
    #     start
    #     >> download_airport_csv_file()
    #     >> create_ods_airport_table()
    #     >> write_airport_csv_to_ods_airport()
    #     >> csv_load
    # )
    #
    # (
    #     start
    #     >> reduce(lambda x, y: x >> y, load_weather_csv_gz_to_db_tasks)
    #     >> create_ods_weather_table()
    #     >> load_weather_csv_gz_to_db_tasks
    #     >> csv_load
    # )
    #
    # (
    #     start
    #     >> copy_all_flight_files()
    #     >> reduce(lambda x, y: x >> y, load_flight_csv_to_db_tasks)
    #     >> create_ods_flight_table()
    #     >> load_flight_csv_to_db_tasks
    #     >> csv_load
    # )
    #
    # (
    #     csv_load
    #     >> create_stg_dds_schemas_and_stg_weather()
    #     >> incremental_fill_stg_weather()
    #     >> create_dds_weather_and_fill_it_as_an_scd()
    #     >> stop
    # )

    (
        start
        >> download_airport_csv_file()
        >> create_ods_tables()
        >> create_dds_airport_plus_tz_tables()
        >> write_airport_csv_to_dds_airport()
        >> write_airport_tz_csv_to_dds_airport()
        >> copy_from_gzip_to_ods_weather()
        >> copy_all_flight_files()
        >> copy_from_csv_to_ods_flight()
        >> stop
    )



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
    print(WEATHER_CSV_GZ_FILE_DICT)

    # rows1 = get_lines_from_gzip(CSV_GZ_FILE)
    # lines1 = get_lines_from_gzip(CSV_GZ_FILE, ICAO_CODE, stop_line=9)
    # rows1, dml1 = get_table_dml_and_rows_for_insert(lines1)
    # for row1 in lines1:
    #     print(row1)
