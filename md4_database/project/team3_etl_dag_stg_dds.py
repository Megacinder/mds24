# aws s3 cp team3_step1_download_airport_csv.py s3://gsbdwhdata/ahremenko_ma/team3_step1_download_airport_csv.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net
# aws s3 cp team3_step1_download_airport_csv.py s3://gsb2024airflow/team3_step1_download_airport_csv.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net
# aws s3 cp team3_etl_dag123.py s3://gsb2024airflow/team3_etl_dag123.py --profile dbdwh --endpoint-url=https://storage.yandexcloud.net

class Metadata:
    version = 3
    type = 'stg and dds'


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
    dag_id="ahremenko_ma_team_3_etl_dag_stg_dds",
    dag_tag="team_3",
    pg_conn_id="con_dwh_2024_s004",
    pg_db_name="dwh_2024_s004",
)

AIRPORT_CSV_PATH = f"{ETL_PARAM["root_file_path"]}/airports.csv"
AIRPORT_TZ_CSV_PATH = f"{ETL_PARAM["root_file_path"]}/airport_tz.csv"


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


SQL_STG = """
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
    ,hash                                 text
    ,load_dt                              timestamp
)
;
create index if not exists ix_stg_weather_icao_dt on stg.weather (icao_code, dt)
--create index ix_stg_weather_icao_dt_hash on stg.weather (icao_code, dt, hash)
;


--drop table if exists stg.flight
--;
create table if not exists stg.flight (
     carrier_code             text
    ,tail_num                 text
    ,carrier_flight_num       text
    ,orig_airport_iata_code   text
    ,dt                       date

    ,year                     int
    ,month                    int
    ,orig_city_name           text
    ,dest_airport_iata_code   text
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
    ,cancel_flg               smallint
    ,cancellation_code        text
    ,flights_cnt              int
    ,distance                 numeric
    ,distance_group_num       numeric
    ,carrier_delay_min        int
    ,weather_delay_min        int
    ,nas_delay_min            int
    ,security_delay_min       int
    ,late_aircraft_delay_min  int
    ,hash                     text
    ,load_dt                  timestamp
)
;

create index if not exists ix_flight_possible_pk on stg.flight (
     carrier_code
    ,tail_num
    ,carrier_flight_num
    ,orig_airport_iata_code
    ,dt
)
;


with wt_ods as (
    select distinct
         coalesce(a.icao_code, '')  as icao_code
        ,coalesce(a.local_time, '1900-01-01') :: timestamp  as dt
        --,case when a.local_time = '' then null else a.local_time end :: timestamp  as dt
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

,wt_hash as (
    select
        -- it can be two equal records: with icao_code = 'KFSM' and local_time = '27.10.2024 04:53'
        -- but with different other values
         icao_code
        ,dt
        ,temperature_cels_deegree
        ,pressure_station_merc_mlm
        ,pressure_see_level_merc_mlm
        ,humidity_prc
        ,wind_direction
        ,wind_speed_meters_per_sec
        ,max_gust_10m_meters_per_sec
        ,special_present_weather_phenomena
        ,recent_weather_phenomena_operational
        ,cloud_cover
        ,horizontal_visibility_km
        ,dewpoint_temperature_cels_deegree

        ,concat_ws(
             '::'
            ,coalesce(temperature_cels_deegree              :: text, '')
            ,coalesce(pressure_station_merc_mlm             :: text, '')
            ,coalesce(pressure_see_level_merc_mlm           :: text, '')
            ,coalesce(humidity_prc                          :: text, '')
            ,coalesce(wind_direction                        :: text, '')
            ,coalesce(wind_speed_meters_per_sec             :: text, '')
            ,coalesce(max_gust_10m_meters_per_sec           :: text, '')
            ,coalesce(special_present_weather_phenomena     :: text, '')
            ,coalesce(recent_weather_phenomena_operational  :: text, '')
            ,coalesce(cloud_cover                           :: text, '')
            ,coalesce(horizontal_visibility_km              :: text, '')
            ,coalesce(dewpoint_temperature_cels_deegree     :: text, '')
        )  as hash

        ,load_dt
    from
        wt_ods_dq_fixing
    where 1=1
)

,wt_delete_and_insert as (
    select
        sou1.*
    from
        wt_hash  sou1
        left join stg.weather  tar1
            on  tar1.icao_code = sou1.icao_code
            and tar1.dt        = sou1.dt
    where 1=1
        and (
            tar1.hash is null
            or (
                tar1.hash is not null
                and tar1.hash != sou1.hash
            )
        )
)

,wt_delete as (
    delete
    from
        stg.weather  tar1
    using
        wt_delete_and_insert  sou1
    where 1=1
        and tar1.icao_code = sou1.icao_code
        and tar1.dt        = sou1.dt
    returning tar1.*
)

insert into stg.weather
select
    *
from
    wt_delete_and_insert
where 1=1
;


-- explain analyze
with wt_ods as (
    select distinct
         round(year  :: numeric, 2)                               :: int       as year
        ,round(month :: numeric, 2)                               :: int       as month

        -- { this is the future pk - there shouldn't be null values here!
        ,to_date(coalesce(flight_dt , '01/01/1900'), 'mm/dd/yyyy hh:mi:ss') :: date      as dt
        ,coalesce(carrier_code      , '')                         :: text      as carrier_code
        ,coalesce(tail_num          , '')                         :: text      as tail_num
        ,coalesce(carrier_flight_num, '')                         :: text      as carrier_flight_num
        ,coalesce(origin_code       , '')                         :: text      as orig_airport_iata_code
        -- }

        ,origin_city_name                                         :: text      as orig_city_name
        ,dest_code                                                :: text      as dest_airport_iata_code
        ,dest_city_name                                           :: text      as dest_city_name
        ,scheduled_dep_tm                                         :: text      as scheduled_dep_tm
        ,actual_dep_tm                                            :: text      as actual_dep_tm
        ,round(dep_delay_min :: numeric, 2)                       :: int       as dep_delay_min
        ,dep_delay_group_num                                      :: numeric   as dep_delay_group_num
        ,wheels_off_tm                                            :: text      as wheels_off_tm
        ,wheels_on_tm                                             :: text      as wheels_on_tm
        ,scheduled_arr_tm                                         :: text      as scheduled_arr_tm
        ,actual_arr_tm                                            :: text      as actual_arr_tm
        ,round(arr_delay_min :: numeric, 2)                       :: int       as arr_delay_min
        ,arr_delay_group_num                                      :: numeric   as arr_delay_group_num
        ,round(coalesce(cancelled_flg, '0') :: numeric, 2)        :: smallint  as cancel_flg
        ,cancellation_code                                        :: text      as cancellation_code
        ,round(flights_cnt :: numeric, 2)                         :: int       as flights_cnt
        ,distance                                                 :: numeric   as distance
        ,distance_group_num                                       :: numeric   as distance_group_num
        ,round(carrier_delay_min       :: numeric, 2)             :: int       as carrier_delay_min
        ,round(weather_delay_min       :: numeric, 2)             :: int       as weather_delay_min
        ,round(nas_delay_min           :: numeric, 2)             :: int       as nas_delay_min
        ,round(security_delay_min      :: numeric, 2)             :: int       as security_delay_min
        ,round(late_aircraft_delay_min :: numeric, 2)             :: int       as late_aircraft_delay_min
        ,localtimestamp  as load_dt
    from
        ods.flight
    where 1=1
)

,wt_hash as (
    select
         carrier_code
        ,tail_num
        ,carrier_flight_num
        ,orig_airport_iata_code
        ,dt

        ,year
        ,month
        ,orig_city_name
        ,dest_airport_iata_code
        ,dest_city_name
        ,scheduled_dep_tm
        ,actual_dep_tm
        ,dep_delay_min
        ,dep_delay_group_num
        ,wheels_off_tm
        ,wheels_on_tm
        ,scheduled_arr_tm
        ,actual_arr_tm
        ,arr_delay_min
        ,arr_delay_group_num
        ,cancel_flg
        ,cancellation_code
        ,flights_cnt
        ,distance
        ,distance_group_num
        ,carrier_delay_min
        ,weather_delay_min
        ,nas_delay_min
        ,security_delay_min
        ,late_aircraft_delay_min

        ,concat_ws(
            '::'
            ,coalesce(year                    :: text, '')
            ,coalesce(month                   :: text, '')
            ,coalesce(orig_city_name          :: text, '')
            ,coalesce(dest_airport_iata_code  :: text, '')
            ,coalesce(dest_city_name          :: text, '')
            ,coalesce(scheduled_dep_tm        :: text, '')
            ,coalesce(actual_dep_tm           :: text, '')
            ,coalesce(dep_delay_min           :: text, '')
            ,coalesce(dep_delay_group_num     :: text, '')
            ,coalesce(wheels_off_tm           :: text, '')
            ,coalesce(wheels_on_tm            :: text, '')
            ,coalesce(scheduled_arr_tm        :: text, '')
            ,coalesce(actual_arr_tm           :: text, '')
            ,coalesce(arr_delay_min           :: text, '')
            ,coalesce(arr_delay_group_num     :: text, '')
            ,coalesce(cancel_flg              :: text, '')
            ,coalesce(cancellation_code       :: text, '')
            ,coalesce(flights_cnt             :: text, '')
            ,coalesce(distance                :: text, '')
            ,coalesce(distance_group_num      :: text, '')
            ,coalesce(carrier_delay_min       :: text, '')
            ,coalesce(weather_delay_min       :: text, '')
            ,coalesce(nas_delay_min           :: text, '')
            ,coalesce(security_delay_min      :: text, '')
            ,coalesce(late_aircraft_delay_min :: text, '')
        )  as hash

        ,load_dt
    from
        wt_ods
)

,wt_delete_and_insert as (
    select
        sou1.*
    from
        wt_hash  sou1
        left join stg.flight  tar1
            on  tar1.carrier_code           = sou1.carrier_code
            and tar1.tail_num               = sou1.tail_num
            and tar1.carrier_flight_num     = sou1.carrier_flight_num
            and tar1.orig_airport_iata_code = sou1.orig_airport_iata_code
            and tar1.dt                     = sou1.dt
    where 1=1
        and (
            tar1.hash is null
            or (
                tar1.hash is not null
                and tar1.hash != sou1.hash
            )
        )
)

,wt_delete as (
    delete
    from
        stg.flight  tar1
    using
        wt_delete_and_insert  sou1
    where 1=1
        and tar1.carrier_code           = sou1.carrier_code
        and tar1.tail_num               = sou1.tail_num
        and tar1.carrier_flight_num     = sou1.carrier_flight_num
        and tar1.orig_airport_iata_code = sou1.orig_airport_iata_code
        and tar1.dt                     = sou1.dt
    returning tar1.*
)

insert into stg.flight
select
    *
from
    wt_delete_and_insert
where 1=1
;
"""


# ======================================================================================================================
#                                            ::::    ::: :::::::::: :::       :::      ::::::::   ::::::::   :::
#                                            :+:+:   :+: :+:        :+:       :+:     :+:    :+: :+:    :+:  :+:
#                                            :+:+:+  +:+ +:+        +:+       +:+     +:+        +:+    +:+  +:+
#                                            +#+ +:+ +#+ +#++:++#   +#+  +:+  +#+     +#++:++#++ +#+    +:+  +#+
#                                            +#+  +#+#+# +#+        +#+ +#+#+ +#+            +#+ +#+  # +#+  +#+
#                                            #+#   #+#+# #+#         #+#+# #+#+#      #+#    #+# #+#   +#+   #+#
#                                            ###    #### ##########   ###   ###        ########   ###### ### ##########
# ======================================================================================================================


SQL_DDS = """

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
create unique index if not exists ix_uniq_dds_airport_icao on dds.airport (icao_code)
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


--drop table if exists dds.weather
--;
create table if not exists dds.weather (
     airport_rk     text        -- ID аэропорта Подставляется из dds_dict.airport (справочник аэропортов) по известному ICAO коду (icao_code)
    ,w_speed        numeric     -- Скорость ветра Значение из поля w_speed
    ,max_gws        numeric     -- Макс. порывы ветра Значение из поля max_gws
    ,t_deg          numeric     -- Температура Значение из поля T
    ,w_phenomena    text        -- MA: добавлено дополнительно (для слоя MDS)
    ,ws_phenomena   text        -- MA: добавлено дополнительно (для слоя MDS)
    ,valid_from     timestamp   -- Начало периода Значение из поля loctime
    ,valid_to       timestamp   -- Окончание периода Следующее известное время сбора данных для этого аэропорта или дата в будущем 5999-01-01
    ,hash           text
    ,load_dt        timestamp  -- Время загрузки Время data interval end
)
;


--drop table if exists dds.flight
--;
create table if not exists dds.flight (
     flight_pk           text
    ,carrier_code        text
    ,carrier_flight_num  text
    ,tail_num            text
    ,orig_airport_id     text
    ,dt                  date
    ,cancel_flg          smallint
    ,dest_airport_id     text
    ,cancel_cd           text
    ,origin_city_name    text
    ,dest_city_name      text
    ,dep_delay_min       int
    ,dep_delay_group_num numeric
    ,arr_delay_min       int
    ,arr_delay_group_num numeric
    ,flights_cnt         int
    ,distance            numeric
    ,distance_group_num  numeric
    ,scheduled_dep_dt    timestamp
    ,actual_dep_dt       timestamp
    ,take_off_dt         timestamp
    ,landing_dt          timestamp
    ,scheduled_arr_dt    timestamp
    ,actual_arr_dt       timestamp
    ,delay_reason_list   text
    ,hash                text
    ,load_dt             timestamp
    ,constraint dds_flight_pk primary key (flight_pk)
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
        ,airport1.id  as airport_rk
        ,stg1.dt

        ,stg1.wind_speed_meters_per_sec    as w_speed
        ,stg1.max_gust_10m_meters_per_sec  as max_gws
        ,stg1.temperature_cels_degree      as t_deg

        ,stg1.special_present_weather_phenomena     as w_phenomena
        ,stg1.recent_weather_phenomena_operational  as ws_phenomena

		-- ,md5(  -- doesnt' work - two equal values for different rows !!!
		--        coalesce(stg1.wind_speed_meters_per_sec   :: text, '')
		--     || coalesce(stg1.max_gust_10m_meters_per_sec :: text, '')
		--     || coalesce(stg1.temperature_cels_degree    :: text, '')
		-- )  as hash

        ,concat_ws(
             '::'
            ,coalesce(stg1.wind_speed_meters_per_sec            :: text, '')
            ,coalesce(stg1.max_gust_10m_meters_per_sec          :: text, '')
            ,coalesce(stg1.temperature_cels_degree              :: text, '')
            ,coalesce(stg1.special_present_weather_phenomena    :: text, '')
            ,coalesce(stg1.recent_weather_phenomena_operational :: text, '')
        )  as hash

        ,localtimestamp  as load_dt
    from
        stg.weather  stg1

        join wt_par  pa1
            on  (pa1.airport_arr is null or stg1.icao_code = any(pa1.airport_arr))
            and (pa1.from_dt     is null or stg1.dt       >= pa1.from_dt         )
            and (pa1.to_dt       is null or stg1.dt       <= pa1.to_dt + 1       )

        join dds.airport  airport1
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
        ,a.w_phenomena
        ,a.ws_phenomena
        -- ,md5(  -- doesn't work - two equal values for different rows !!!
        --        coalesce(w_speed :: text, '')
        --     || coalesce(max_gws :: text, '')
        --     || coalesce(t_deg   :: text, '')
        -- )  as hash
        ,concat_ws(
             '::'
            ,coalesce(a.w_speed      :: text, '')
            ,coalesce(a.max_gws      :: text, '')
            ,coalesce(a.t_deg        :: text, '')
            ,coalesce(a.w_phenomena  :: text, '')
            ,coalesce(a.ws_phenomena :: text, '')
        )  as hash

        -- if the data exists than this is the old data - don't change the load_dt
        ,a.load_dt  as load_dt
    from
        dds.weather  a
        left join wt_stg_table  b
            on  b.airport_rk = a.airport_rk
            and b.dt         = a.valid_from
            and b.hash       = a.hash
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
        ,(array_agg(w_speed      order by priority))[1]  as w_speed
        ,(array_agg(max_gws      order by priority))[1]  as max_gws
        ,(array_agg(t_deg        order by priority))[1]  as t_deg
        ,(array_agg(w_phenomena  order by priority))[1]  as w_phenomena
        ,(array_agg(ws_phenomena order by priority))[1]  as ws_phenomena

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
        ,w_phenomena
        ,ws_phenomena
        ,hash
    from
        wt_stg_table_wo_dds
    where 1=1
)


,wt_to_insert as (
    select
         period1.airport_rk

        ,value1.w_speed
        ,value1.max_gws
        ,value1.t_deg
        ,value1.w_phenomena
        ,value1.ws_phenomena

        ,period1.valid_from
        ,period1.valid_to
        ,period1.hash
        ,period1.load_dt
    --    ,period1.collapsed_row_cnt
    from
        wt_valid_period  period1

        join wt_distinct_values  value1
            on  value1.airport_rk = period1.airport_rk
            and value1.hash       = period1.hash
    where 1=1
)


,wt_to_delete as (
    select
        airport_rk
        ,min(valid_from)  as min_valid_from
        ,max(valid_from)  as max_valid_from
    from
        wt_to_insert
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
    *
from
    wt_to_insert
where 1=1
;


--explain analyze
with wt_par as (
    select
        --array[0, 1, 2, 3] :: int[]  as cancel_flg
        array[0, 1] :: int[]  as cancel_flg
)


,wt_raw as (
    -- we add all columns because of the Inmon architecture
    select
         stg1.carrier_code
        ,stg1.carrier_flight_num
        ,stg1.tail_num
        ,stg1.cancel_flg
        ,stg1.dt

        ,stg1.dt at time zone coalesce(orig_airport_tz1.tz, 'utc')  as orig_dt
        ,stg1.dt at time zone coalesce(dest_airport_tz1.tz, 'utc')  as dest_dt

        ,left(stg1.scheduled_dep_tm, 2) :: int * interval '1 hour' + substr(stg1.scheduled_dep_tm, 3) :: int * interval '1 min'  as scheduled_dep_iv
        ,left(stg1.actual_dep_tm,    2) :: int * interval '1 hour' + substr(stg1.actual_dep_tm,    3) :: int * interval '1 min'  as actual_dep_iv
        ,left(stg1.wheels_off_tm,    2) :: int * interval '1 hour' + substr(stg1.wheels_off_tm,    3) :: int * interval '1 min'  as take_off_iv
        ,left(stg1.wheels_on_tm,     2) :: int * interval '1 hour' + substr(stg1.wheels_on_tm,     3) :: int * interval '1 min'  as landing_iv
        ,left(stg1.scheduled_arr_tm, 2) :: int * interval '1 hour' + substr(stg1.scheduled_arr_tm, 3) :: int * interval '1 min'  as scheduled_arr_iv
        ,left(stg1.actual_arr_tm,    2) :: int * interval '1 hour' + substr(stg1.actual_arr_tm,    3) :: int * interval '1 min'  as actual_arr_iv


        ,stg1.cancellation_code  as cancel_cd
        ,orig_airport1.id  as orig_airport_id
        ,dest_airport1.id  as dest_airport_id


--         ,to_timestamp(
--             to_char(
--                 case
--                     when left(stg1.actual_dep_tm, 2) :: int >= 24
--                         then
--                             stg1.dt + 1
--                     else
--                         stg1.dt
--                 end
--                 ,'yyyy-mm-dd'
--             )
--
--             || ' ' || case
--                 when left(stg1.actual_dep_tm, 2) :: int >= 24
--                     then
--                         (left(stg1.actual_dep_tm, 2) :: int - 24) :: text
--                     else
--                         left(stg1.actual_dep_tm, 2)
--             end
--             || ':' || substr(stg1.actual_dep_tm, 3)
--             ,'yyyy-mm-dd hh24:mi'
--         ) at time zone coalesce(orig_airport_tz1.tz, 'utc')  as dep_local_dttm

        ,stg1.orig_airport_iata_code
        ,stg1.orig_city_name
        ,stg1.dest_airport_iata_code
        ,stg1.dest_city_name

        ,stg1.dep_delay_min
        ,stg1.dep_delay_group_num
        ,stg1.arr_delay_min
        ,stg1.arr_delay_group_num

        ,stg1.flights_cnt
        ,stg1.distance
        ,stg1.distance_group_num

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

        --join wt_par  pa1
        --    on stg1.cancel_flg = any(pa1.cancel_flg)

        join dds.airport  orig_airport1
            on orig_airport1.iata_code = stg1.orig_airport_iata_code

        left join dds.airport  dest_airport1
            on dest_airport1.iata_code = stg1.dest_airport_iata_code

        left join dds.airport_tz  orig_airport_tz1
            on orig_airport_tz1.iata_code = orig_airport1.iata_code

        left join dds.airport_tz  dest_airport_tz1
            on dest_airport_tz1.iata_code = dest_airport1.iata_code
    where 1=1
        --and carrier_code || '.' || tail_num || '.' || carrier_flight_num = '9E.N311PQ.4930'
)

,wt_delay_reason as (
    select
         carrier_code
        ,carrier_flight_num
        ,tail_num
        ,cancel_flg
        ,dt

        --,orig_dt  -- not sure if it is needed
        --,dest_dt  -- not sure if it is needed

        ,cancel_cd
        ,orig_airport_id
        ,dest_airport_id

        --,origin_code
        ,orig_city_name
        --,dest_code
        ,dest_city_name
        ,dep_delay_min
        ,dep_delay_group_num
        ,arr_delay_min
        ,arr_delay_group_num
        ,flights_cnt
        ,distance
        ,distance_group_num

        ,load_dt

        ,orig_dt + scheduled_dep_iv  as scheduled_dep_dt
        ,orig_dt + actual_dep_iv     as actual_dep_dt
        ,orig_dt + take_off_iv       as take_off_dt
        ,dest_dt + landing_iv        as landing_dt
        ,dest_dt + scheduled_arr_iv  as scheduled_arr_dt
        ,dest_dt + actual_arr_iv     as actual_arr_dt

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
        || '.' || coalesce(carrier_flight_num :: text, '0')
        || '.' || coalesce(tail_num           :: text, '0')
        || '.' || coalesce(orig_airport_id    :: text, '0')
        || '.' || coalesce(dt                 :: text, '1900-01-01')
        as flight_pk

        ,carrier_code
        ,carrier_flight_num
        ,tail_num
        ,orig_airport_id
        ,dt

        ,cancel_flg
        ,dest_airport_id
        ,cancel_cd
        ,orig_city_name
        ,dest_city_name
        ,dep_delay_min
        ,dep_delay_group_num
        ,arr_delay_min
        ,arr_delay_group_num
        ,flights_cnt
        ,distance
        ,distance_group_num

        ,scheduled_dep_dt
        ,actual_dep_dt
        ,take_off_dt
        ,landing_dt
        ,scheduled_arr_dt
        ,actual_arr_dt

        ,delay_reason_list

        ,concat_ws(
             '::'
            ,coalesce(cancel_flg          :: text, '')
            ,coalesce(dest_airport_id     :: text, '')
            ,coalesce(cancel_cd           :: text, '')
            ,coalesce(orig_city_name      :: text, '')
            ,coalesce(dest_city_name      :: text, '')
            ,coalesce(dep_delay_min       :: text, '')
            ,coalesce(dep_delay_group_num :: text, '')
            ,coalesce(arr_delay_min       :: text, '')
            ,coalesce(arr_delay_group_num :: text, '')
            ,coalesce(flights_cnt         :: text, '')
            ,coalesce(distance            :: text, '')
            ,coalesce(distance_group_num  :: text, '')
            ,coalesce(scheduled_dep_dt    :: text, '')
            ,coalesce(actual_dep_dt       :: text, '')
            ,coalesce(take_off_dt         :: text, '')
            ,coalesce(landing_dt          :: text, '')
            ,coalesce(scheduled_arr_dt    :: text, '')
            ,coalesce(actual_arr_dt       :: text, '')
            ,coalesce(delay_reason_list   :: text, '')
        )  as hash

        ,load_dt
    from
        wt_delay_reason
    where 1=1
        --and carrier_code || '.' || tail_num || '.' || carrier_flight_num = '9E.N311PQ.4930'
)

,wt_delete_and_insert as (
    select
        sou1.*
    from
        wt_hash  sou1
        left join dds.flight  tar1
            on  tar1.flight_pk = sou1.flight_pk
    where 1=1
        and (
            tar1.hash is null
            or (
                tar1.hash is not null
                and tar1.hash != sou1.hash
            )
        )
)

,wt_delete as (
    delete
    from
        dds.flight  tar1
    using
        wt_delete_and_insert  sou1
    where 1=1
        and tar1.flight_pk = sou1.flight_pk
    returning tar1.*
)

insert into dds.flight
select
    *
from
    wt_delete_and_insert
where 1=1
;


drop table if exists dds.flight_successful
;
create table if not exists dds.flight_successful (
     carrier_flight_num text          -- Номер рейса перевозчика
    ,flight_dttm_local  timestamp     -- Локальное время и дата вылета
    ,origin_airport_dk  text          -- Код аэропорта отправления

    ,dest_airport_dk    text[]        -- Код аэропорта назначения
    ,carrier_code       text[]        -- Код перевозчика
    ,tail_num           text[]        -- Бортовой номер самолета
    ,distance_m         numeric[]     -- Дальность полета (мили)
    ,dep_delay_min      int[]         -- Задержка вылета (минуты)
    ,delay_reasons_code text[]        -- можно вместо этого вывести все поля с задержками Причины задержки. Расчетное поле
    ,hash               text
    ,processed_dttm     timestamp     -- Время вставки записи
    ,constraint flight_successful_pk primary key (carrier_flight_num, flight_dttm_local, origin_airport_dk)
)
;


drop table if exists dds.flight_cancelled
;
create table if not exists dds.flight_cancelled (
     carrier_flight_num text        -- Номер рейса перевозчика
    ,sched_dttm_local   timestamp   -- Локальное время и дата вылета
    ,origin_airport_dk  text        -- Код аэропорта отправления
    ,dest_airport_dk    text        -- Код аэропорта назначения
    ,carrier_code       text        -- Код перевозчика
    ,cancellation_code  text        -- Код причины отмены рейса
    ,hash               text
    ,processed_dttm     timestamp   -- Время вставки записи
    ,constraint flight_cancelled_pk primary key (carrier_flight_num, sched_dttm_local, origin_airport_dk)
)
;


with wt_par as (
    select
        --array[0, 1, 2, 3] :: int[]  as cancel_flg
        array[0] :: int[]  as cancel_flg
)

,wt_dds_flight_succ as (
    select
        -- PK
         coalesce(a.carrier_flight_num, '')  as carrier_flight_num
        ,coalesce(a.actual_dep_dt     , '1900-01-01')  as flight_dttm_local
        ,coalesce(a.orig_airport_id   , '')  as origin_airport_dk


        ,array_agg(a.dest_airport_id  )  as dest_airport_dk
        ,array_agg(a.carrier_code     )  as carrier_code
        ,array_agg(a.tail_num         )  as tail_num
        ,array_agg(a.distance         )  as distance_m
        ,array_agg(a.dep_delay_min    )  as dep_delay_min
        ,array_agg(a.delay_reason_list)  as delay_reasons_code

        ,localtimestamp  as load_dt
    from
        dds.flight  a
        join wt_par  pa1
            on a.cancel_flg = any(pa1.cancel_flg)
    where 1=1
    group by
         coalesce(a.carrier_flight_num, '')
        ,coalesce(a.actual_dep_dt     , '1900-01-01')
        ,coalesce(a.orig_airport_id   , '')
)

,wt_hash as (
    select
         carrier_flight_num
        ,flight_dttm_local
        ,origin_airport_dk


        ,dest_airport_dk
        ,carrier_code
        ,tail_num
        ,distance_m
        ,dep_delay_min
        ,delay_reasons_code

        ,concat_ws(
             '::'
            ,coalesce(dest_airport_dk    :: text, '')
            ,coalesce(carrier_code       :: text, '')
            ,coalesce(tail_num           :: text, '')
            ,coalesce(distance_m         :: text, '')
            ,coalesce(dep_delay_min      :: text, '')
            ,coalesce(delay_reasons_code :: text, '')
        )  as hash

        ,load_dt
    from
        wt_dds_flight_succ
    where 1=1
)

,wt_delete_and_insert as (
    select
        sou1.*
    from
        wt_hash  sou1
        left join dds.flight_successful  tar1
            on  tar1.carrier_flight_num = sou1.carrier_flight_num
            and tar1.flight_dttm_local  = sou1.flight_dttm_local
            and tar1.origin_airport_dk  = sou1.origin_airport_dk
    where 1=1
        and (
            tar1.hash is null
            or (
                tar1.hash is not null
                and tar1.hash != sou1.hash
            )
        )
)

,wt_delete as (
    delete
    from
        dds.flight_successful  tar1
    using
        wt_delete_and_insert  sou1
    where 1=1
        and tar1.carrier_flight_num = sou1.carrier_flight_num
        and tar1.flight_dttm_local  = sou1.flight_dttm_local
        and tar1.origin_airport_dk  = sou1.origin_airport_dk
        --and tar1.hash              != sou1.hash
    returning tar1.*
)

insert into dds.flight_successful
select
    *
from
    wt_delete_and_insert
where 1=1
;




with wt_par as (
    select
        --array[0, 1, 2, 3] :: int[]  as cancel_flg
        array[1] :: int[]  as cancel_flg
)

,wt_dds_flight_canc as (
    select
        -- PK
         coalesce(a.carrier_flight_num, '')  as carrier_flight_num
        ,coalesce(a.scheduled_dep_dt  , '1900-01-01')  as sched_dttm_local
        ,coalesce(a.orig_airport_id   , '')  as origin_airport_dk

        ,array_agg(a.dest_airport_id  )  as dest_airport_dk
        ,array_agg(a.carrier_code     )  as carrier_code
        ,array_agg(a.cancel_cd        )  as cancellation_code

        ,localtimestamp  as load_dt
    from
        dds.flight  a
        join wt_par  pa1
            on a.cancel_flg = any(pa1.cancel_flg)
    where 1=1
    group by
         coalesce(a.carrier_flight_num, '')
        ,coalesce(a.scheduled_dep_dt  , '1900-01-01')
        ,coalesce(a.orig_airport_id   , '')
)

,wt_hash as (
    select
         carrier_flight_num
        ,sched_dttm_local
        ,origin_airport_dk


        ,dest_airport_dk
        ,carrier_code
        ,cancellation_code

        ,concat_ws(
             '::'
            ,coalesce(dest_airport_dk   :: text, '')
            ,coalesce(carrier_code      :: text, '')
            ,coalesce(cancellation_code :: text, '')
        )  as hash

        ,load_dt
    from
        wt_dds_flight_canc
    where 1=1
)

,wt_delete_and_insert as (
    select
        sou1.*
    from
        wt_hash  sou1
        left join dds.flight_cancelled  tar1
            on  tar1.carrier_flight_num = sou1.carrier_flight_num
            and tar1.sched_dttm_local   = sou1.sched_dttm_local
            and tar1.origin_airport_dk  = sou1.origin_airport_dk
    where 1=1
        and (
            tar1.hash is null
            or (
                tar1.hash is not null
                and tar1.hash != sou1.hash
            )
        )
)

,wt_delete as (
    delete
    from
        dds.flight_cancelled  tar1
    using
        wt_delete_and_insert  sou1
    where 1=1
        and tar1.carrier_flight_num = sou1.carrier_flight_num
        and tar1.sched_dttm_local   = sou1.sched_dttm_local
        and tar1.origin_airport_dk  = sou1.origin_airport_dk
        --and tar1.hash              != sou1.hash
    returning tar1.*
)

insert into dds.flight_cancelled
select
    *
from
    wt_delete_and_insert
where 1=1
;
"""


# ======================================================================================================================
#                                            ::::    ::: :::::::::: :::       :::      ::::::::   ::::::::   :::
#                                            :+:+:   :+: :+:        :+:       :+:     :+:    :+: :+:    :+:  :+:
#                                            :+:+:+  +:+ +:+        +:+       +:+     +:+        +:+    +:+  +:+
#                                            +#+ +:+ +#+ +#++:++#   +#+  +:+  +#+     +#++:++#++ +#+    +:+  +#+
#                                            +#+  +#+#+# +#+        +#+ +#+#+ +#+            +#+ +#+  # +#+  +#+
#                                            #+#   #+#+# #+#         #+#+# #+#+#      #+#    #+# #+#   +#+   #+#
#                                            ###    #### ##########   ###   ###        ########   ###### ### ##########
# ======================================================================================================================


SQL_MDS = """
create schema if not exists mds;


--drop table if exists mds.airport_weather
--;
create table if not exists mds.airport_weather (
     rain_flg           int  --  Дождь w_phenomena включает 'rain' или ws_phenomena включает 'rain'
    ,snow_flg           int  --  Снег w_phenomena включает 'snow' или ws_phenomena включает 'snow'
    ,thunderstorm_flg   int  --  Гроза w_phenomena включает 'thunderstorm' или ws_phenomena включает 'thunderstorm'
    ,fog_mist_flg       int  --  Туман/Мгла w_phenomena включает 'fog' или 'mist' или ws_phenomena включает 'fog' или 'mist'
    ,drizzle_flg        int  --  Морось w_phenomena включает 'drizzle' или ws_phenomena включает 'drizzle'
    ,freezing_flg       int  --  Холодно t < 0
    ,w_speed            int  --  Скорость ветра Значение из поля w_speed
    ,max_gws            int  --  Макс. порывы ветра Значение из поля max_gws
    ,t_deg              int  --  Температура Значение из поля T
    ,airport_rk         int  --  ID аэропорта вылета Подставляется из dds.airport по известному ICAO коду (icao_code)
    ,flights_cnt        int  --  Количество вылетевших рейсов
    ,delay_min_avg      int  --  Средняя задержка вылета
    ,valid_from_dttm    int  --  Начало периода Значение из поля loctime
    ,valid_to_dttm      int  --  Окончание периода Следующее известное время сбора данных для этого аэропорта или дата в будущем 5999-01-01
    --,processed_dttm     int  --  Время загрузки Время data interval end
    ,load_dt            int  --  Время загрузки Время data interval end
    ,constraint mds_airport_weather_pk primary key (airport_rk, valid_from_dttm)
)
;


with wt_flag as (
    select
         a.airport_rk
        ,a.valid_from  as dt

        ,case when lower(a.w_phenomena) like       '%rain%'         or lower(a.ws_phenomena) like       '%rain%'         then 1 else 0 end  as rain_flg
        ,case when lower(a.w_phenomena) like       '%snow%'         or lower(a.ws_phenomena) like       '%snow%'         then 1 else 0 end  as snow_flg
        ,case when lower(a.w_phenomena) like       '%thunderstorm%' or lower(a.ws_phenomena) like       '%thunderstorm%' then 1 else 0 end  as thunderstorm_flg
        ,case when lower(a.w_phenomena) similar to '%fog%|%mist%'   or lower(a.ws_phenomena) similar to '%fog%|%mist%'   then 1 else 0 end  as fog_mist_flg
        ,case when lower(a.w_phenomena) like       '%drizzle%'      or lower(a.ws_phenomena) like       '%drizzle%'      then 1 else 0 end  as drizzle_flg

        ,case when a.t_deg < 0 then 1 else 0 end freezing_flg

        ,a.w_speed
        ,a.max_gws
        ,a.t_deg
        ,localtimestamp  as load_dt
    from
        dds.weather  a
    where 1=1
)


,wt_hash as (
    select
        a.*
        ,concat_ws(
             '::'
            ,coalesce(a.rain_flg         :: text, '')
            ,coalesce(a.snow_flg         :: text, '')
            ,coalesce(a.thunderstorm_flg :: text, '')
            ,coalesce(a.fog_mist_flg     :: text, '')
            ,coalesce(a.drizzle_flg      :: text, '')
            ,coalesce(a.freezing_flg     :: text, '')
            ,coalesce(a.w_speed          :: text, '')
            ,coalesce(a.max_gws          :: text, '')
            ,coalesce(a.t_deg            :: text, '')
        )  as hash
    from
        wt_flag  a
    where 1=1
)


,wt_prev_hash as (
    select
        airport_rk
        ,dt
        ,hash
        ,load_dt
        ,lag(hash, 1, hash) over (partition by airport_rk order by dt)  as prev_hash
    from
        wt_hash
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

        ,rain_flg
        ,snow_flg
        ,thunderstorm_flg
        ,fog_mist_flg
        ,drizzle_flg
        ,freezing_flg

        ,w_speed
        ,max_gws
        ,t_deg

        ,hash
    from
        wt_hash
    where 1=1
)


,wt_new_period as (
    select
         period1.airport_rk

        ,value1.rain_flg
        ,value1.snow_flg
        ,value1.thunderstorm_flg
        ,value1.fog_mist_flg
        ,value1.drizzle_flg
        ,value1.freezing_flg

        ,value1.w_speed
        ,value1.max_gws
        ,value1.t_deg

        ,period1.valid_from
        ,period1.valid_to
        ,period1.hash
        ,period1.load_dt
    --    ,period1.collapsed_row_cnt
    from
        wt_valid_period  period1

        join wt_distinct_values  value1
            on  value1.airport_rk = period1.airport_rk
            and value1.hash       = period1.hash
    where 1=1
)

select
     a.rain_flg
    ,a.snow_flg
    ,a.thunderstorm_flg
    ,a.fog_mist_flg
    ,a.drizzle_flg
    ,a.freezing_flg

    ,a.w_speed
    ,a.max_gws
    ,a.t_deg
    ,a.airport_rk

    ,count(1)  as flight_cnt
    ,avg(b.dep_delay_min)  as delay_min_avg
    ,a.valid_from
    ,a.valid_to
from
    wt_new_period  a
    left join dds.flight  b
        on b.orig_airport_id = a.airport_rk
        and b.actual_dep_dt >= a.valid_from
        and b.actual_dep_dt <= a.valid_to
where 1=1
group by
     a.rain_flg
    ,a.snow_flg
    ,a.thunderstorm_flg
    ,a.fog_mist_flg
    ,a.drizzle_flg
    ,a.freezing_flg

    ,a.w_speed
    ,a.max_gws
    ,a.t_deg

    ,a.airport_rk
    ,a.valid_from
    ,a.valid_to
;
"""


@dag(**DAG_PARAM)
def dag1() -> None:
    """
    the dev team 3 etl
    """
    start = EmptyOperator(task_id="start")
    stop = EmptyOperator(task_id="stop")
    # start_csv_load = EmptyOperator(task_id="start_csv_load")
    # stop_csv_load = EmptyOperator(task_id="stop_csv_load")


    @task.python(do_xcom_push=False)
    def stg__create_and_fill_tables() -> (XComArg | None):
        """
        create stg schema and tables if needed and fills them
        :return:
        """
        with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
            logger.info("Start refreshing stg tables..")
            cur = conn.cursor()
            cur.execute(SQL_STG)
            conn.commit()
            logger.debug("stg tables refreshed")



    @task.python(do_xcom_push=False)
    def dds__create_and_fill_tables() -> (XComArg | None):
        """
        create dds schema and tables if needed and fills them
        :return:
        """
        with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
            logger.debug("Start refreshing dds tables ..")
            cur = conn.cursor()
            cur.execute(SQL_DDS)
            conn.commit()
            logger.debug("dds tables refreshed")


    @task.python(do_xcom_push=False)
    def mds__create_and_fill_tables() -> (XComArg | None):
        """
        create dds schema and tables if needed and fills them
        :return:
        """
        with PostgresHook(postgres_conn_id=ETL_PARAM["pg_conn_id"]).get_conn() as conn:
            logger.debug("Start refreshing dds tables ..")
            cur = conn.cursor()
            cur.execute(SQL_DDS)
            conn.commit()
            logger.debug("dds tables refreshed")

    (
        start
        >> stg__create_and_fill_tables()
        >> dds__create_and_fill_tables()
        >> mds__create_and_fill_tables()
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
