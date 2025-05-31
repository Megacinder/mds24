select
    ident
    ,icao_code
    ,iata_code
from
    ods.airport
where 1=1
    and iata_code in ('FSM', 'XNA', 'YUM', 'FLG')


https://rp5.ru/Weather_archive_in_Flagstaff_(airport),_METAR
https://rp5.ru/Weather_archive_in_Fort_Smith_(airport),_USA,_METAR
https://rp5.ru/Weather_archive_in_Yuma_(airport),_METAR
https://rp5.ru/Weather_archive_in_Northwest_Arkansas_(airport),_METAR

https://ru1.rp5.ru/download/files.metar/KX/KXNA.14.05.2025.21.05.2025.1.0.0.en.utf8.00000000.csv.gz


select
     icao_code
    ,case when local_time = '' then null else local_time end ::timestamp  as dt
    ,case when raw_t      = '' then null else raw_t      end ::numeric    as temperatu_celc_deegree
    ,case when raw_p0     = '' then null else raw_p0     end ::numeric    as pressure_station_merc_mlm
    ,case when raw_p      = '' then null else raw_p      end ::numeric    as pressure_see_level_merc_mlm
    ,case when raw_u      = '' then null else raw_u      end ::numeric    as humidity_prc
    ,case when raw_dd     = '' then null else raw_dd     end              as wind_direction
    ,case when raw_ff     = '' then null else raw_ff     end ::numeric    as wind_speed_meters_per_sec
    ,case when raw_ff10   = '' then null else raw_ff10   end              as max_gust_10m_meters_per_sec
    ,case when raw_ww     = '' then null else raw_ww     end              as special_present_weather_phenomena
    ,case when raw_w_w_   = '' then null else raw_w_w_   end              as recent_weather_phenomena_operational
    ,case when raw_c      = '' then null else raw_c      end              as cloud_cover
    ,case when raw_vv     = '' then null else raw_vv     end ::numeric    as horizontal_visibility_km
    ,case when raw_td     = '' then null else raw_td     end ::numeric    as dewpoint_temperature_celc_deegree
from
    ods.weather
where 1=1


create schema stg
create schema dds






create table stg.weather as
select
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
    ,md5(
           a.raw_t    || a.raw_p0 || a.raw_p    || a.raw_u || a.raw_dd || a.raw_ff
        || a.raw_ff10 || a.raw_ww || a.raw_w_w_ || a.raw_c || a.raw_vv || a.raw_td
    )  as hash
from
    ods.weather  a
where 1=1


create index ix_icao_dt on stg.weather (icao_code, dt)


create table dds.weather (
     airport_rk text       -- ID аэропорта Подставляется из dds_dict.airport (справочник аэропортов) по известному ICAO коду (icao_code)
    ,w_speed    numeric    -- Скорость ветра Значение из поля w_speed
    ,max_gws    numeric    -- Макс. порывы ветра Значение из поля max_gws
    ,t_deg      numeric    -- Температура Значение из поля T
    ,valid_from timestamp  -- Начало периода Значение из поля loctime
    ,valid_to   timestamp  -- Окончание периода Следующее известное время сбора данных для этого аэропорта или дата в будущем 5999-01-01
    ,load_dt    timestamp  -- Время загрузки Время data interval end
)






/*
select
    string_agg(column_name, ' || ' order by ordinal_position)  as col_list
from
    information_schema.columns
where 1=1
    and table_name = 'weather'
    and column_name != 'local_time'
*/


with wt_par as (
    select
        '{KFLG}' :: text[]  as airport_arr
        ,date'2024-12-01'   as from_dt
        ,date'2024-12-11'   as to_dt

--         null :: text[]  as airport_arr
--        ,null :: date  as from_dt
--        ,null :: date  as to_dt
)


,wt_norm_col_name_w_hash as (
    select
         a.icao_code
        ,dt
        ,temperature_celc_deegree
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
        ,dewpoint_temperature_celc_deegree
        ,hash
    from
        stg.weather  a
        cross join wt_par  pa1
    where 1=1
        and (pa1.airport_arr is null or a.icao_code = any(pa1.airport_arr))
        and (pa1.from_dt     is null or a.dt       >= pa1.from_dt         )
        and (pa1.to_dt       is null or a.dt       <= pa1.to_dt + 1       )

)


,wt_prev_hash as (
    select
        icao_code
        ,dt
        ,hash
        ,lag(hash, 1, hash) over (partition by icao_code order by dt)  as prev_hash
    from
        wt_norm_col_name_w_hash
    where 1=1
)

--select count(1) from wt_prev_hash

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
        ,sum(change_flg) over (partition by icao_code order by dt, hash)  as group_key
    from
        wt_change_flg
    where 1=1
)

--select count(1) from wt_group_key

,wt_valid_period as (
    select
        icao_code
        ,hash
        ,min(dt)  as valid_from
        ,lead(
            min(dt)
            ,1
            ,'6000-01-01' :: timestamp
        ) over (
            partition by
                icao_code
            order by
                min(dt)
        ) - interval '1 millisecond'  as valid_to
    from
        wt_group_key
    where 1=1
        --and group_key = '7050'
    group by
        icao_code
        ,hash
        ,group_key
)

--select count(1) from wt_valid_period

,wt_distinct_values as (
    select distinct  -- unique hash and his values
        hash
        ,icao_code
        ,temperature_celc_deegree
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
        ,dewpoint_temperature_celc_deegree
    from
        wt_norm_col_name_w_hash
    where 1=1
)

--select count(1) from wt_distinct_values

--insert into dds.weather
,wt_fin as (
    select
         airport1.id :: text || '.' || period1.icao_code  as airport_rk

        ,values1.wind_speed_meters_per_sec  as w_speed
        ,values1.max_gust_10m_meters_per_sec  as max_gws
        ,values1.temperature_celc_deegree  as t_deg

        ,period1.valid_from
        ,period1.valid_to
        ,localtimestamp  as load_dt
    from
        wt_valid_period  period1

        join wt_distinct_values  values1
            on  values1.icao_code = period1.icao_code
            and values1.hash      = period1.hash

        join ods.airport  airport1
            on airport1.icao_code = period1.icao_code
    where 1=1
)

--insert into dds.weather
select
    stg1.*
from
    wt_fin  stg1
    left join dds.weather  dds1
        on  dds1.airport_rk = stg1.airport_rk
        and dds1.valid_from = stg1.valid_from
        and dds1.valid_to   = stg1.valid_to
where 1=1
    and dds1.airport_rk is null
--select * from dds.weather


--select group_key, count(1) from wt_group_key
--group by group_key having count(1) > 1 order by count(1) desc


--select
--    icao_code
--    ,hash
--    ,min(dt)  as valid_from
--    ,max(dt)  as valid_to
--from
--    wt_group_key
--group by
--    icao_code
--    ,hash
--    ,group_key
--order by
--    icao_code
--    ,min(dt)





--,wt_sou as (
--select * from wt_hash
----where hash in (select hash from wt_change_flg where change_flg = 1)
--where hash = '7864174591dc73a8ee6ca24502764bf4'
--)
--
--select * from wt_a
--select
--    hash_key
--    ,count(1)
--from
--    wt_hash
--group by
--    hash_key
--having 1=1
--    and count(1) > 1

--select
--    string_agg(column_name, ', ' order by ordinal_position)  as col_list
--from
--    information_schema.columns
--where table_name = 'weather'




select
    flight_dt
    ,tail_num
    ,carrier_flight_num
    ,actual_dep_tm
    ,count(1)
from
    ods.flight
where 1=1
group by
    flight_dt
    ,tail_num
    ,carrier_flight_num
    ,actual_dep_tm
having count(1) > 1


-- aws s3 cp C:\stuff\hse\modul4_databases\airport_tz.csv s3://gsbdwhdata/ahremenko_ma/airport_tz.csv --endpoint-url=https://storage.yandexcloud.net --profile=dbdwh