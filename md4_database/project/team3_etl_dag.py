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

AIRPORT_CSV_URL = "https://ourairports.com/data/airports.csv"
AIRPORT_CSV_PATH = f"{ROOT_FILE_PATH}/airports.csv"
TARGET_CSV_BUCKET = "gsbdwhdata"


FLIGHT_SOURCE_BUCKET = "db01-content"
FLIGHT_SOURCE_PATH = "flights"
FLIGHT_FILE_NAME_TEMPLATE = "T_ONTIME_REPORTING-"

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
    dag_id=f"ahremenko_ma_{DAG_TAG}_etl",
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
    csv_load = EmptyOperator(task_id="csv_load")

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
    def create_ods_airport_table() -> (XComArg | None):
        """
        creates a table for airport data from the previous task
        :return: nothing, but actually airflow's XComArg
        """
        file_path = s3_hook.download_file(
            key=AIRPORT_CSV_PATH,
            bucket_name=TARGET_CSV_BUCKET,
        )

        with open(file_path, "r", encoding="utf-8") as file:
            csv_reader_ex = csv_reader(file)
            headers = next(csv_reader_ex)


        columns = [f'"{header}" TEXT' for header in headers]
        sql = f"""
            create table if not exists {ODS_AIRPORT_TABLE} (
                {', '.join(columns)}
            );
        """

        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn().cursor() as cur:
            cur.execute(sql)
            logger.info("Table created successfully")


    @task.python(do_xcom_push=False)
    def write_airport_csv_to_ods_airport() -> (XComArg | None):
        """
        write the airport data to the table from the tasks
        :return: nothing, but actually airflow's XComArg
        """

        file_path = s3_hook.download_file(
            key=AIRPORT_CSV_PATH,
            bucket_name=TARGET_CSV_BUCKET,
        )

        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn().cursor() as cur:
            if file_path:
                sql = f"truncate table {ODS_AIRPORT_TABLE}"
                cur.execute(sql)

                with open(file_path, "r") as file:
                    sql_pg_copy_csv = f"copy {ODS_AIRPORT_TABLE} from stdin with csv header delimiter as ',' quote '\"'"
                    cur.copy_expert(sql_pg_copy_csv, file)


    @task.python(do_xcom_push=False)
    def copy_all_flight_files() -> (XComArg | None):
        all_files = s3_hook.list_keys(
            bucket_name=FLIGHT_SOURCE_BUCKET,
            prefix=f"{FLIGHT_SOURCE_PATH}/"
        )

        matching_files = [
            file for file in all_files
            if file.startswith(f"{FLIGHT_SOURCE_PATH}/{FLIGHT_FILE_NAME_TEMPLATE}") and file.endswith('.csv')
        ]

        for source_key in matching_files:
            file_id = source_key.split(FLIGHT_FILE_NAME_TEMPLATE)[1].replace('.csv', '')
            destination_key = f"{ROOT_FILE_PATH}/flight_{file_id}.csv"

            file_data = s3_hook.read_key(source_key, FLIGHT_SOURCE_BUCKET)
            s3_hook.load_string(
                string_data=file_data,
                key=destination_key,
                bucket_name=TARGET_CSV_BUCKET,
                replace=True
            )


    @task.python(do_xcom_push=False)
    def create_ods_weather_table_and_truncate() -> (XComArg | None):
        """
        creates ods.weather and truncate (if exists, truncate only)
        :return:
        """
        header_lines = get_lines_from_gzip(file_path, icao_code, stop_line=8)

        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn().cursor() as cur:
            sql = get_table_ddl(header_lines)
            logger.info(sql)
            cur.execute(sql)

            sql = f"truncate table {ODS_WEATHER_TABLE}"
            logger.info(sql)
            cur.execute(sql)

    load_airport_csv_gz_to_db_tasks = []

    for icao_code, csv_gz_file in WEATHER_CSV_GZ_FILE_DICT.items():
        file_path = s3_hook.download_file(
            key=csv_gz_file,
            bucket_name=TARGET_CSV_BUCKET,
        )
        def create_load_to_ods_airport_data_task(icao_code: str, file_path: str) -> (XComArg | None):
            """
            wrapper to a list of parallel files' loading
            :param icao_code:
            :param file_path:
            :return:
            """
            @task.python(do_xcom_push=True, task_id=f"load_to_ods_airport_data_for_{icao_code}")
            def insert_rows_from_gzip() -> (XComArg | None):
                """
                insert weather data to a database's table in parallel - from 4 files simultaneously
                :return: nothing, but actually airflow's XComArg
                """
                rows = get_lines_from_gzip(file_path, icao_code, start_line=7)

                with NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as tmp:
                    csv_writer_ex = csv_writer(tmp)
                    csv_writer_ex.writerows(rows)
                    tmp_path = tmp.name


                with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn().cursor() as cur:
                    with open(tmp_path, 'r') as file:
                        cur.execute("SELECT current_database(), current_schema()")
                        db, schema = cur.fetchone()
                        logger.info(f"Current DB: {db}, Current schema: {schema}")
                        sql_pg_copy_csv = f"copy {ODS_WEATHER_TABLE} from stdin with csv delimiter as ',' quote '\"'"
                        cur.copy_expert(sql_pg_copy_csv, file)

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

        task1 = create_load_to_ods_airport_data_task(icao_code, file_path)
        load_airport_csv_gz_to_db_tasks.append(task1)



    @task.python(do_xcom_push=False)
    def create_stg_weather_and_fill_it() -> (XComArg | None):
        """
        creates stg.weather and fill it
        :return:
        """
        sql = """
        create schema if not exists stg;
        create schema if not exists dds;


        create index if not exists ix_ods_airport_icao on ods.airport (icao_code);


        create table if not exists stg.weather (
             icao_code                            text
            ,dt                                   timestamp
            ,temperature_celc_deegree             numeric
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
            ,dewpoint_temperature_celc_deegree    numeric
        --     ,hash                                 text
        )
        ;


        truncate table stg.weather
        ;

        insert into stg.weather
        select distinct
             a.icao_code
            ,case when a.local_time = '' then null else a.local_time end :: timestamp  as dt
            ,case when a.raw_t      = '' then null else a.raw_t      end :: numeric    as temperature_celc_deegree
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
            ,case when a.raw_td     = '' then null else a.raw_td     end :: numeric    as dewpoint_temperature_celc_deegree
        --     ,md5(
        --            a.raw_t    || a.raw_p0 || a.raw_p    || a.raw_u || a.raw_dd || a.raw_ff
        --         || a.raw_ff10 || a.raw_ww || a.raw_w_w_ || a.raw_c || a.raw_vv || a.raw_td
        --     )  as hash
        from
            ods.weather  a
        where 1=1
        ;
        
        
        create index if not exists ix_stg_weather_icao_dt on stg.weather (icao_code, dt)
        --create index ix_stg_weather_icao_dt_hash on stg.weather (icao_code, dt, hash)
        ;
        """
        logger.info(sql)

        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn().cursor() as cur:
            cur.execute(sql)



    @task.python(do_xcom_push=False)
    def create_dds_weather_and_fill_it_as_an_scd() -> (XComArg | None):
        """
        create dds.weather and fill it - with SCD, blackjack and whores
        :return:
        """
        sql = """
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
                ,stg1.temperature_celc_deegree     as t_deg
        
                -- ,md5(  -- doesnt' work - two equal values for different rows !!!
                --        coalesce(stg1.wind_speed_meters_per_sec   :: text, '')
                --     || coalesce(stg1.max_gust_10m_meters_per_sec :: text, '')
                --     || coalesce(stg1.temperature_celc_deegree    :: text, '')
                -- )  as hash
        
                ,concat_ws(
                     '::'
                    ,coalesce(stg1.wind_speed_meters_per_sec   :: text, '')
                    ,coalesce(stg1.max_gust_10m_meters_per_sec :: text, '')
                    ,coalesce(stg1.temperature_celc_deegree    :: text, '')
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
            returning *
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
        ;
        """
        logger.info(sql)
        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn().cursor() as cur:
            cur.execute(sql)


    (
        start
        >> download_airport_csv_file()
        >> create_ods_airport_table()
        >> write_airport_csv_to_ods_airport()
        >> csv_load
    )

    (
        start
        >> create_ods_weather_table_and_truncate()
        >> load_airport_csv_gz_to_db_tasks
        >> csv_load
    )

    (
        csv_load
        >> create_stg_weather_and_fill_it()
        >> create_dds_weather_and_fill_it_as_an_scd()
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
