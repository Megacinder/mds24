# aws s3 cp team3_step1_download_airport_csv.py s3://gsbdwhdata/ahremenko_ma/team3_step1_download_airport_csv.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net
# aws s3 cp team3_step1_download_airport_csv.py s3://gsb2024airflow/team3_step1_download_airport_csv.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net
# aws s3 cp team3_etl_dag.py s3://gsb2024airflow/team3_etl_dag.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net

class Metadata:
    version = 7
    type = 'full'


from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.xcom_arg import XComArg
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

from csv import reader as csv_reader, writer as csv_writer
from datetime import datetime
from functools import reduce
from gzip import open as gzip_open
from logging import getLogger, Filter, WARNING
from os import path, unlink, environ
from tempfile import NamedTemporaryFile
from typing import Generator, Any, List


class SuppressRequestsFilter(Filter):
    """
    cut logs - so that a task doesn't shit in the logs
    """
    def filter(self, record):
        return not record.name.startswith("urllib3")


ETL_PARAM = dict(
    root_file_path="ahremenko_ma",  # airport and flight data in my - ahremenko_ma - path only
    dag_id="ahremenko_ma_team_3_etl_dag",
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
    # "flight_2024-04.csv",
    # "flight_2024-05.csv",
    # "flight_2024-06.csv",
    # "flight_2024-07.csv",
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
ODS_AIRPORT_TABLE = "ods.airport"
ODS_AIRPORT_TZ_TABLE = "ods.airport_tz"
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


def get_ods_airport_table_ddl(lines: List[list]) -> str:
    """
    gives table ddl
    :param lines: lines of CSV file as list of lists
    :return: table ddl
    """
    headers = lines[0]
    headers = ["icao_code", "local_time"] + [f"raw_{col_name.lower()}" for col_name in headers[2:]]

    columns = [f'"{header}" TEXT' for header in headers]
    create_table_sql = f"""
        create table if not exists {ODS_WEATHER_TABLE} (
            {', '.join(columns)}
        );
    """

    return create_table_sql


@dag(**DAG_PARAM)
def dag1() -> None:
    """
    the dev team 3 etl
    """
    start = EmptyOperator(task_id="start")
    stop = EmptyOperator(task_id="stop")
    start_csv_load = EmptyOperator(task_id="start_csv_load")
    stop_csv_load = EmptyOperator(task_id="stop_csv_load")


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
        creates a table for airport data from the previous task
        :return: nothing, but actually airflow's XComArg
        """
        sql = """
        create schema if not exists ods
        ;
        
        
        drop table if exists ods.flight
        ;
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
        
        
        drop table if exists ods.airport
        ;
        create table if not exists ods.airport (
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
        --create index if not exists ix_ods_airport_icao on ods.airport using btree (icao_code)
        --;


        drop table if exists ods.airport_tz
        ;
        create table if not exists ods.airport_tz (
            iata_code text
            ,tz       text
        )
        ;


        drop table if exists ods.weather
        ;
        create table ods.weather (
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
            logger.info("ODS scheme and tables there were created")


    @task.python(do_xcom_push=False)
    def write_airport_csv_to_ods_airport() -> (XComArg | None):
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
                    sql_pg_copy_csv = f"copy {ODS_AIRPORT_TABLE} from stdin with csv header delimiter as ',' quote '\"'"
                    cur.copy_expert(sql_pg_copy_csv, file)
                    conn.commit()


    @task.python(do_xcom_push=False)
    def write_airport_tz_csv_to_ods_airport() -> (XComArg | None):
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
                    sql_pg_copy_csv = f"copy {ODS_AIRPORT_TZ_TABLE} from stdin with csv header delimiter as ',' quote '\"'"
                    cur.copy_expert(sql_pg_copy_csv, file)
                    conn.commit()


    load_weather_csv_gz_to_db_tasks = []

    for icao_code, csv_gz_file in WEATHER_CSV_GZ_FILE_DICT.items():
        if IS_LOCAL:
            return
        else:
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            file_path = s3_hook.download_file(
                key=csv_gz_file,
                bucket_name=TARGET_CSV_BUCKET,
            )
        def create_load_to_ods_weather_data_task(icao_code: str, file_path: str) -> (XComArg | None):
            """
            wrapper to a list of parallel airport files' loading
            :param icao_code:
            :param file_path:
            :return:
            """
            @task.python(do_xcom_push=True, task_id=f"load_to_ods_weather_data_for_{icao_code}")
            def insert_rows_from_gzip() -> (XComArg | None):
                """
                insert weather data to a database's table in parallel - from 4 files simultaneously
                :return: nothing, but actually airflow's XComArg
                """
                if IS_LOCAL: return

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
                        cur.execute("SELECT current_database(), current_schema()")
                        db, schema = cur.fetchone()
                        logger.info(f"Current DB: {db}, Current schema: {schema}")
                        sql_pg_copy_csv = f"copy {ODS_WEATHER_TABLE} from stdin with csv delimiter as ',' quote '\"'"
                        cur.copy_expert(sql_pg_copy_csv, file)
                        conn.commit()

                        # logger.info(rows[0])
                        # for row in rows:
                        #     sql = f"""
                        #         insert into {ODS_WEATHER_TABLE}
                        #         values (
                        #             {"'" + "', '".join(row) + "'"}
                        #         )
                        #     """
                        #     cur.executemany(sql, rows)
                        # logger.info(rows[-1])

                if tmp_path and path.exists(tmp_path):
                    unlink(tmp_path)


            return insert_rows_from_gzip()

        task1 = create_load_to_ods_weather_data_task(icao_code, file_path)
        load_weather_csv_gz_to_db_tasks.append(task1)


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


    load_flight_csv_to_db_tasks = []

    for csv_file in FLIGHT_CSV_FILE_LIST:
        if IS_LOCAL:
            return
        else:
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            file_path = s3_hook.download_file(
                key=csv_file,
                bucket_name=TARGET_CSV_BUCKET,
            )
        def create_load_to_ods_flight_data_task(file_path: str) -> (XComArg | None):
            """
            wrapper to a list of parallel flight files' loading
            :param file_path:
            :return:
            """
            task_id_name = (
                f"load_to_ods_flight_data_for_{csv_file.split("_")[-1].replace(".csv", "").replace("-", "")}"
            )
            @task.python(do_xcom_push=True, task_id=task_id_name)
            def insert_rows_from_csv() -> (XComArg | None):
                """
                insert flight data to a database's table in parallel - from several files simultaneously
                :return: nothing, but actually airflow's XComArg
                """
                logger_task = getLogger("airflow.task")
                logger_task.setLevel(WARNING)
                logger_task.addFilter(SuppressRequestsFilter())

                with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
                    with open(file_path, 'r') as file:
                        cur = conn.cursor()
                        sql_pg_copy_csv = f"copy {ODS_FLIGHT_TABLE} from stdin with csv header delimiter as ',' quote '\"'"
                        cur.copy_expert(sql_pg_copy_csv, file)
                        conn.commit()


            return insert_rows_from_csv()

        task2 = create_load_to_ods_flight_data_task(file_path)
        load_flight_csv_to_db_tasks.append(task2)

    @task.python(do_xcom_push=False)
    def create_and_fill_stg_tables() -> (XComArg | None):
        """
        create stg schema and tables if needed and fills them
        :return:
        """
        sql = """
        create schema if not exists stg
        ;


        --drop table if exists stg.weather
        --;
        create table if not exists stg.weather (
             icao_code                            text
            ,dt                                   timestamp
            ,temperature_cels_degree              numeric
            ,pressure_station_merc_mlm            numeric
            ,pressure_see_level_merc_mlm          numeric
            ,humidity_prc                         numeric
            ,wind_direction                       text
            ,wind_speed_meters_per_sec            numeric
            ,max_gust_10m_meters_per_sec          numeric
            ,special_present_weather_phenomena    text
            ,recent_weather_phenomena_operational text
            ,cloud_cover                          text
            ,horizontal_visibility_km             numeric
            ,dewpoint_temperature_cels_deegree    numeric
            ,load_dt                              timestamp
        --     ,hash                                 text
        )
        ;


        --drop table if exists stg.flight
        --;
        create table if not exists stg.flight (
             year                     int
            ,month                    int
            ,dt                       date
            ,carrier_code             text
            ,tail_num                 text
            ,carrier_flight_num       text
            ,origin_code              text
            ,origin_city_name         text
            ,dest_code                text
            ,dest_city_name           text
            ,scheduled_dep_tm         text
            ,actual_dep_tm            text
            ,dep_delay_min            int
            ,dep_delay_group_num      numeric
            ,wheels_off_tm            text
            ,wheels_on_tm             text
            ,scheduled_arr_tm         text
            ,actual_arr_tm            text
            ,arr_delay_min            int
            ,arr_delay_group_num      numeric
            ,cancelled_flg            smallint
            ,cancellation_code        text
            ,flights_cnt              int
            ,distance                 numeric
            ,distance_group_num       numeric
            ,carrier_delay_min        int
            ,weather_delay_min        int
            ,nas_delay_min            int
            ,security_delay_min       int
            ,late_aircraft_delay_min  int
            ,load_dt                  timestamp
        )
        ;


        -- fill stg.weather
        with wt_ods as (
            select distinct
                 a.icao_code
                ,case when a.local_time = '' then null else a.local_time end :: timestamp  as dt
                ,case when a.raw_t      = '' then null else a.raw_t      end :: numeric    as temperature_cels_deegree
                ,case when a.raw_p0     = '' then null else a.raw_p0     end :: numeric    as pressure_station_merc_mlm
                ,case when a.raw_p      = '' then null else a.raw_p      end :: numeric    as pressure_see_level_merc_mlm
                ,case when a.raw_u      = '' then null else a.raw_u      end :: numeric    as humidity_prc
                ,case when a.raw_dd     = '' then null else a.raw_dd     end               as wind_direction
                ,case when a.raw_ff     = '' then null else a.raw_ff     end :: numeric    as wind_speed_meters_per_sec
                ,case when a.raw_ff10   = '' then null else a.raw_ff10   end :: numeric    as max_gust_10m_meters_per_sec
                ,case when a.raw_ww     = '' then null else a.raw_ww     end               as special_present_weather_phenomena
                ,case when a.raw_w_w_   = '' then null else a.raw_w_w_   end               as recent_weather_phenomena_operational
                ,case when a.raw_c      = '' then null else a.raw_c      end               as cloud_cover
                ,case when a.raw_vv     = '' then null else a.raw_vv     end :: numeric    as horizontal_visibility_km
                ,case when a.raw_td     = '' then null else a.raw_td     end :: numeric    as dewpoint_temperature_cels_deegree
                ,localtimestamp  as load_dt
            --     ,md5(
            --            a.raw_t    || a.raw_p0 || a.raw_p    || a.raw_u || a.raw_dd || a.raw_ff
            --         || a.raw_ff10 || a.raw_ww || a.raw_w_w_ || a.raw_c || a.raw_vv || a.raw_td
            --     )  as hash
            from
                ods.weather  a
            where 1=1
        )

        ,wt_ods_dq_fixing as (
            select
                -- it can be two equal records: with icao_code = 'KFSM' and local_time = '27.10.2024 04:53'
                -- but with different other values
                 icao_code
                ,dt
                ,max(temperature_cels_deegree)              as temperature_cels_deegree
                ,max(pressure_station_merc_mlm)             as pressure_station_merc_mlm
                ,max(pressure_see_level_merc_mlm)           as pressure_see_level_merc_mlm
                ,max(humidity_prc)                          as humidity_prc
                ,max(wind_direction)                        as wind_direction
                ,max(wind_speed_meters_per_sec)             as wind_speed_meters_per_sec
                ,max(max_gust_10m_meters_per_sec)           as max_gust_10m_meters_per_sec
                ,max(special_present_weather_phenomena)     as special_present_weather_phenomena
                ,max(recent_weather_phenomena_operational)  as recent_weather_phenomena_operational
                ,max(cloud_cover)                           as cloud_cover
                ,max(horizontal_visibility_km)              as horizontal_visibility_km
                ,max(dewpoint_temperature_cels_deegree)     as dewpoint_temperature_cels_deegree
                ,load_dt
            from
                wt_ods
            where 1=1
            group by
                 icao_code
                ,dt
                ,load_dt
        )

        ,wt_to_delete as (
            select
                icao_code
                ,min(dt)  as min_dt
                ,max(dt)  as max_dt
            from
                wt_ods_dq_fixing
            where 1=1
            group by
                icao_code
        )

        ,wt_delete as (
            delete
            from
                stg.weather  tar1
            using
                wt_to_delete  sou1
            where 1=1
                and tar1.icao_code = sou1.icao_code
                and tar1.dt between sou1.min_dt and sou1.max_dt
            returning tar1.*
        )

        insert into stg.weather
        select
            *
        from
            wt_ods_dq_fixing
        where 1=1
        --     and (icao_code, dt) not in (
        --         select
        --             icao_code, dt
        --         from
        --             wt_delete
        --     )
        ;

        create index if not exists ix_stg_weather_icao_dt on stg.weather (icao_code, dt)
        --create index ix_stg_weather_icao_dt_hash on stg.weather (icao_code, dt, hash)
        ;



        -- fill stg.flight
        with wt_ods as (
            select distinct
                 year                                      :: int       as year
                ,month                                     :: int       as month
                ,to_date(flight_dt, 'mm/dd/yyyy hh:mi:ss') :: date      as dt
                ,carrier_code                              :: text      as carrier_code
                ,tail_num                                  :: text      as tail_num
                ,carrier_flight_num                        :: text      as carrier_flight_num
                ,origin_code                               :: text      as origin_airport_iata_code
                ,origin_city_name                          :: text      as origin_city_name
                ,dest_code                                 :: text      as dest_airport_iata_code
                ,dest_city_name                            :: text      as dest_city_name
                ,scheduled_dep_tm                          :: text      as scheduled_dep_tm
                ,actual_dep_tm                             :: text      as actual_dep_tm
                ,dep_delay_min                             :: int       as dep_delay_min
                ,dep_delay_group_num                       :: numeric   as dep_delay_group_num
                ,wheels_off_tm                             :: text      as wheels_off_tm
                ,wheels_on_tm                              :: text      as wheels_on_tm
                ,scheduled_arr_tm                          :: text      as scheduled_arr_tm
                ,actual_arr_tm                             :: text      as actual_arr_tm
                ,arr_delay_min                             :: int       as arr_delay_min
                ,arr_delay_group_num                       :: numeric   as arr_delay_group_num
                ,coalesce(cancelled_flg, 0)                :: smallint  as cancelled_flg
                ,cancellation_code                         :: text      as cancellation_code
                ,flights_cnt                               :: int       as flights_cnt
                ,distance                                  :: numeric   as distance
                ,distance_group_num                        :: numeric   as distance_group_num
                ,carrier_delay_min                         :: int       as carrier_delay_min
                ,weather_delay_min                         :: int       as weather_delay_min
                ,nas_delay_min                             :: int       as nas_delay_min
                ,security_delay_min                        :: int       as security_delay_min
                ,late_aircraft_delay_min                   :: int       as late_aircraft_delay_min
                ,localtimestamp  as load_dt
            from
                ods.flights
            where 1=1
        )

        ,wt_to_delete as (
            -- there are a lot of doubles - a single flight can be cancelled several times per day
            --  so we will delete all data for the period
            select
                 min(dt)  as min_dt
                ,max(dt)  as max_dt
            from
                wt_ods
            where 1=1
        )

        ,wt_delete as (
            delete
            from
                stg.flight  tar1
            using
                wt_to_delete  sou1
            where 1=1
                and tar1.dt between sou1.min_dt and sou1.max_dt
            returning tar1.*
        )

        insert into stg.flight
        select
            *
        from
            wt_ods
        where 1=1
        --     and dt not in (
        --         select
        --             dt
        --         from
        --             wt_delete
        --     )
        ;


        """
        with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
            logger.info("Start refreshing STG tables..")
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            logger.debug("DDS tables refreshed")

    @task.python(do_xcom_push=False)
    def create_dds_tables_and_fill_them() -> (XComArg | None):
        """
        create dds schema and tables if needed and fills them
        :return:
        """
        sql = """
        create schema if not exists dds;


        --drop table if exists dds.weather
        --;
        create table if not exists dds.weather (
             airport_rk text       -- ID аэропорта Подставляется из dds_dict.airport (справочник аэропортов) по известному ICAO коду (icao_code)
            ,w_speed    numeric    -- Скорость ветра Значение из поля w_speed
            ,max_gws    numeric    -- Макс. порывы ветра Значение из поля max_gws
            ,t_deg      numeric    -- Температура Значение из поля T
            ,valid_from timestamp  -- Начало периода Значение из поля loctime
            ,valid_to   timestamp  -- Окончание периода Следующее известное время сбора данных для этого аэропорта или дата в будущем 5999-01-01
            ,load_dt    timestamp  -- Время загрузки Время data interval end
        )
        ;


        --drop table if exists dds.flight
        --;
        create table if not exists dds.flight (
             id                 text
            ,carrier_code       text
            ,tail_num           text
            ,carrier_flight_num text
            ,origin_airport_id  text
            ,dt                 date
            ,local_dttm         timestamp
            ,cancel_flg         smallint
            ,dest_airport_id    text
            ,distance           int
            ,dep_delay_min      text
            ,delay_reason_list  text
            ,hash               text
            ,load_dt            timestamp
        )
        ;




        --explain analyze
        with wt_par as (
            select
        --        '{KFLG}' :: text[]  as airport_arr
        --        ,date'2024-12-10'   as from_dt
        --        ,date'2024-12-15'   as to_dt

                 null :: text[]  as airport_arr
                ,null :: date  as from_dt
                ,null :: date  as to_dt
        )


        ,wt_stg_table as (
            select
                 1  as priority
                ,airport1.id :: text || '.' || stg1.icao_code  as airport_rk
                ,stg1.dt

                ,stg1.wind_speed_meters_per_sec    as w_speed
                ,stg1.max_gust_10m_meters_per_sec  as max_gws
                ,stg1.temperature_cels_degree      as t_deg

                -- ,md5(  -- doesnt' work - two equal values for different rows !!!
                --        coalesce(stg1.wind_speed_meters_per_sec   :: text, '')
                --     || coalesce(stg1.max_gust_10m_meters_per_sec :: text, '')
                --     || coalesce(stg1.temperature_cels_degree    :: text, '')
                -- )  as hash

                ,concat_ws(
                     '::'
                    ,coalesce(stg1.wind_speed_meters_per_sec   :: text, '')
                    ,coalesce(stg1.max_gust_10m_meters_per_sec :: text, '')
                    ,coalesce(stg1.temperature_cels_degree     :: text, '')
                )  as hash

                ,localtimestamp  as load_dt
            from
                stg.weather  stg1

                join wt_par  pa1
                    on  (pa1.airport_arr is null or stg1.icao_code = any(pa1.airport_arr))
                    and (pa1.from_dt     is null or stg1.dt       >= pa1.from_dt         )
                    and (pa1.to_dt       is null or stg1.dt       <= pa1.to_dt + 1       )

                join ods.airport  airport1
                    on airport1.icao_code = stg1.icao_code
            where 1=1
        )


        ,wt_dds_table as (
            select
                 case when b.airport_rk is not null then 0 else 2 end  as priority
                ,a.airport_rk
                ,a.valid_from  as dt

                ,a.w_speed
                ,a.max_gws
                ,a.t_deg

                -- ,md5(  -- doesnt' work - two equal values for different rows !!!
                --        coalesce(w_speed :: text, '')
                --     || coalesce(max_gws :: text, '')
                --     || coalesce(t_deg   :: text, '')
                -- )  as hash
                ,concat_ws(
                     '::'
                    ,coalesce(a.w_speed :: text, '')
                    ,coalesce(a.max_gws :: text, '')
                    ,coalesce(a.t_deg   :: text, '')
                )  as hash

                -- if the data exists than this is the old data - don't change the load_dt
                ,a.load_dt  as load_dt
            from
                dds.weather  a
                left join wt_stg_table  b
                    on  b.airport_rk = a.airport_rk
                    and b.dt         = a.valid_from
                    and b.hash       = concat_ws(
                         '::'
                        ,coalesce(a.w_speed :: text, '')
                        ,coalesce(a.max_gws :: text, '')
                        ,coalesce(a.t_deg   :: text, '')
                    )
            where 1=1
        )


        ,wt_stg_table_wo_dds as (
            -- union both datasets - new and old, because some periods of values can be changed in the past
            select
                *
            from
                wt_stg_table

            union all

            select
                *
            from
                wt_dds_table  a
            where 1=1
        )


        ,wt_old_period_priority as (
            -- if data has changed in previous period, then we choose new values instead
            select
                airport_rk
                ,dt
                ,(array_agg(w_speed order by priority))[1]  as w_speed
                ,(array_agg(max_gws order by priority))[1]  as max_gws
                ,(array_agg(t_deg   order by priority))[1]  as t_deg
                ,(array_agg(hash    order by priority))[1]  as hash
                ,(array_agg(load_dt order by priority))[1]  as load_dt
                -- ,(array_agg(load_dt order by priority))  as load_dt_arr
                ,count(1)  as array_len
                ,min(priority)  as highest_priority
            from
                wt_stg_table_wo_dds
            group by
                airport_rk
                ,dt
        )


        ,wt_prev_hash as (
            select
                airport_rk
                ,dt
                ,hash
                ,load_dt
                ,lag(hash, 1, hash) over (partition by airport_rk order by dt)  as prev_hash
            from
                wt_old_period_priority
            where 1=1
        )


        ,wt_change_flg as (
            select
                *
                ,case when hash != prev_hash then 1 else 0 end  as change_flg
            from
                wt_prev_hash
            where 1=1
        )


        ,wt_group_key as (
            select
                *
                ,sum(change_flg) over (partition by airport_rk order by dt, hash)  as group_key
            from
                wt_change_flg
            where 1=1
        )


        ,wt_valid_period as (
            select
                airport_rk
                ,hash
                ,min(dt)  as valid_from
                ,count(1)  as collapsed_row_cnt

                ,lead(
                    min(dt)
                    ,1
                    ,'6000-01-01' :: timestamp
                ) over (
                    partition by
                        airport_rk
                    order by
                        min(dt)
                ) - interval '1 millisecond'  as valid_to

                ,max(load_dt)  as load_dt
            from
                wt_group_key
            where 1=1
            group by
                airport_rk
                ,hash
                ,group_key
        )


        ,wt_distinct_values as (
            select distinct  -- unique hash and his values
                airport_rk
                ,w_speed
                ,max_gws
                ,t_deg
                ,hash
            from
                wt_stg_table_wo_dds
            where 1=1
        )


        ,wt_fin as (
            select
                 period1.airport_rk

                ,values1.w_speed
                ,values1.max_gws
                ,values1.t_deg

                ,period1.valid_from
                ,period1.valid_to
                ,period1.load_dt
            --    ,period1.collapsed_row_cnt
            from
                wt_valid_period  period1

                join wt_distinct_values  values1
                    on  values1.airport_rk = period1.airport_rk
                    and values1.hash       = period1.hash
            where 1=1
        )


        ,wt_to_delete as (
            select
                airport_rk
                ,min(valid_from)  as min_valid_from
                ,max(valid_from)  as max_valid_from
            from
                wt_fin
            where 1=1
            group by
                airport_rk
        )


        ,wt_delete as (
            delete
            from
                dds.weather  tar1
            using
                wt_to_delete  sou1
            where 1=1
                and tar1.airport_rk = sou1.airport_rk
                and tar1.valid_from between sou1.min_valid_from and sou1.max_valid_from
            returning tar1.*
        )

        insert into dds.weather
        select
             airport_rk

            ,w_speed
            ,max_gws
            ,t_deg

            ,valid_from
            ,valid_to
            ,load_dt
        from
            wt_fin
        where 1=1
        --     and (airport_rk, valid_from, valid_to) not in (
        --         select
        --             airport_rk, valid_from, valid_to
        --         from wt_delete
        --     )
        ;

        --truncate table dds.weather





        with wt_par as (
            select
                --array[0, 1, 2, 3] :: int[]  as cancel_flg
                array[0, 1] :: int[]  as cancel_flg
        )

        ,wt_raw as (
            select
                 stg1.carrier_flight_num

                ,stg1.carrier_code
                ,stg1.tail_num
                ,stg1.cancelled_flg  as cancel_flg

                ,orig_airport1.id  as origin_airport_id
                ,stg1.dt
                ,stg1.actual_dep_tm

                ,to_timestamp(
                    to_char(
                        case
                            when left(stg1.actual_dep_tm, 2) :: int >= 24
                                then
                                    stg1.dt + 1
                            else
                                stg1.dt
                        end
                        ,'yyyy-mm-dd'
                    )

                    || ' ' || case
                        when left(stg1.actual_dep_tm, 2) :: int >= 24
                            then
                                (left(stg1.actual_dep_tm, 2) :: int - 24) :: text
                            else
                                left(stg1.actual_dep_tm, 2)
                    end
                    || ':' || substr(stg1.actual_dep_tm, 3)
                    ,'yyyy-mm-dd hh24:mi'
                ) at time zone coalesce(orig_airport_tz1.tz, 'utc')  as local_dttm

                ,dest_airport1.id  as dest_airport_id

                ,stg1.distance
                ,stg1.dep_delay_min
                --,carrier_delay_min
                --,weather_delay_min
                --,nas_delay_min
                --,security_delay_min
                --,late_aircraft_delay_min


                ,concat_ws(
                    ', '
                    ,case when coalesce(stg1.carrier_delay_min      , 0) > 0 then 'вина перевозчика' else null end
                    ,case when coalesce(stg1.weather_delay_min      , 0) > 0 then 'погода' else null end
                    ,case when coalesce(stg1.nas_delay_min          , 0) > 0 then 'NAS (National aviation services)' else null end
                    ,case when coalesce(stg1.security_delay_min     , 0) > 0 then 'проверка безопасности' else null end
                    ,case when coalesce(stg1.late_aircraft_delay_min, 0) > 0 then 'позднее прибытие самолета' else null end
                )  as delay_reason
                ,localtimestamp  as load_dt
            from
                stg.flight  stg1

                join wt_par  pa1
                    on stg1.cancelled_flg = any(pa1.cancel_flg)

                join ods.airport  orig_airport1
                    on orig_airport1.iata_code = stg1.origin_code

                join ods.airport  dest_airport1
                    on dest_airport1.iata_code = stg1.dest_code

                left join ods.airport_tz  orig_airport_tz1
                    on orig_airport_tz1.iata_code = orig_airport1.iata_code
            where 1=1
                --and carrier_code || '.' || tail_num || '.' || carrier_flight_num = '9E.N311PQ.4930'
        )

        ,wt_delay_reason as (
            select
                *
                ,case
                    when dep_delay_min > 0 and delay_reason != ''
                        then
                            delay_reason
                    when dep_delay_min > 0 and delay_reason = ''
                        then
                            'неизвестно'
                    else
                        null
                end  as delay_reason_list
            from
                wt_raw
        )

        ,wt_hash as (
            select
                          coalesce(carrier_code       :: text, '0')
                || '.' || coalesce(tail_num           :: text, '0')
                || '.' || coalesce(carrier_flight_num :: text, '0')
                || '.' || coalesce(origin_airport_id  :: text, '0')
                || '.' || coalesce(dt                 :: text, '0')
                as id

                ,carrier_code
                ,tail_num
                ,carrier_flight_num
                ,origin_airport_id
                ,dt
                ,local_dttm

                ,cancel_flg
                ,dest_airport_id
                ,distance
                ,dep_delay_min
                ,delay_reason_list

                ,concat_ws(
                     '::'
                    ,coalesce(cancel_flg        :: text, '')
                    ,coalesce(dest_airport_id   :: text, '')
                    ,coalesce(distance          :: text, '')
                    ,coalesce(dep_delay_min     :: text, '')
                    ,coalesce(delay_reason_list :: text, '')
                )  as hash

                ,load_dt
            from
                wt_delay_reason
            where 1=1
                --and carrier_code || '.' || tail_num || '.' || carrier_flight_num = '9E.N311PQ.4930'
        )


        ,wt_dq as (
            select
                id
                ,count(1)
            from
                wt_hash
            where 1=1
            group by
                id
            having 1=1
                and count(1) > 1
        )


        ,wt_delete as (
            delete
            from
                dds.flight  tar1
            using
                wt_hash  sou1
            where 1=1
                and tar1.id    = sou1.id
                and tar1.hash != sou1.hash
            returning tar1.*
        )

        insert into dds.flight
        select
            *
        from
            wt_hash
        where 1=1
        --     and (id, hash) not in (
        --         select
        --             id, hash
        --         from
        --             wt_delete
        --     )
        ;


        """

        with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
            logger.debug("Start refreshing DDS tables ..")
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            logger.debug("DDS tables refreshed")

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
        >> copy_all_flight_files()
        >> create_ods_tables()
        >> start_csv_load
        >> write_airport_csv_to_ods_airport()
        >> stop_csv_load
    )

    (
        [
            reduce(lambda x, y: x >> y, [start_csv_load] + load_weather_csv_gz_to_db_tasks),
            reduce(lambda x, y: x >> y, [start_csv_load] + load_flight_csv_to_db_tasks),
        ]
        >> stop_csv_load
        >> create_and_fill_stg_tables()
        >> create_dds_tables_and_fill_them()
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
