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
    and (icao_code, dt) not in (
        select
            icao_code, dt
        from
            wt_delete
    )
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
    and dt not in (
        select
            dt
        from
            wt_delete
    )
;

