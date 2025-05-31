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
