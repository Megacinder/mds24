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