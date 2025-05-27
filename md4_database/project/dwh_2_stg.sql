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


create table if not exists stg.flight (
     year                       int
    ,month                      int
    ,dt                         date
    ,carrier_code               text
    ,tail_num                   text
    ,carrier_flight_num         text
    ,origin_airport_iata_code   text
    ,origin_city_name           text
    ,dest_airport_iata_code     text
    ,dest_city_name             text
    ,scheduled_dep_tm           text
    ,actual_dep_tm              text
    ,dep_delay_min              int
    ,dep_delay_group_num        numeric
    ,wheels_off_tm              text
    ,wheels_on_tm               text
    ,scheduled_arr_tm           text
    ,actual_arr_tm              text
    ,arr_delay_min              int
    ,arr_delay_group_num        numeric
    ,cancelled_flg              smallint
    ,cancellation_code          text
    ,flights_cnt                int
    ,distance                   numeric
    ,distance_group_num         numeric
    ,carrier_delay_min          int
    ,weather_delay_min          int
    ,nas_delay_min              int
    ,security_delay_min         int
    ,late_aircraft_delay_min    int
)
;

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

,wt_to_delete as (
    select
        icao_code
        ,min(dt)  as min_dt
        ,max(dt)  as max_dt
    from
        wt_ods
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
    returning *
)

insert into stg.weather
select
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
    ,load_dt
from
    wt_ods
where 1=1
;

create index if not exists ix_stg_weather_icao_dt on stg.weather (icao_code, dt)
--create index ix_stg_weather_icao_dt_hash on stg.weather (icao_code, dt, hash)
;


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

--truncate table dds.weather





--dds.flight_successful
with wt_par as (
    select
        --array[0, 1, 2, 3] :: int[]  as cancel_flg
        array[0] :: int[]  as cancel_flg
)

,wt_flight as (
    select
        carrier_flight_num
        ,carrier_code || '.' || tail_num || '.' || carrier_flight_num  as flight_rk
        ,to_date(flight_dt, 'mm/dd/yyyy hh:mi:ss') :: date  as dt

        ,orig_airport1.id  as origin_airport_dk
        ,dest_airport1.id  as dest_airport_dk

        ,carrier_code
        ,tail_num
        ,distance
        ,dep_delay_min
        --,carrier_delay_min
        --,weather_delay_min
        --,nas_delay_min
        --,security_delay_min
        --,late_aircraft_delay_min
        ,coalesce(cancelled_flg, 0)  as cancel_flg

        ,concat_ws(
            ', '
            ,case when coalesce(carrier_delay_min      , 0) > 0 then 'вина перевозчика' else null end
            ,case when coalesce(weather_delay_min      , 0) > 0 then 'погода' else null end
            ,case when coalesce(nas_delay_min          , 0) > 0 then 'NAS (National aviation services)' else null end
            ,case when coalesce(security_delay_min     , 0) > 0 then 'проверка безопасности' else null end
            ,case when coalesce(late_aircraft_delay_min, 0) > 0 then 'позднее прибытие самолета' else null end
        )  as delay_reason
        ,now()  as load_dt
    from
        ods.flights  flight1
        join wt_par  pa1
            on coalesce(flight1.cancelled_flg, 0) = any(cancel_flg)
        join ods.airport  orig_airport1
            on orig_airport1.iata_code = flight1.origin_code
        join ods.airport  dest_airport1
            on dest_airport1.iata_code = flight1.dest_code
    where 1=1
        --and carrier_code || '.' || tail_num || '.' || carrier_flight_num = '9E.N311PQ.4930'
)

,wt_fin as (
    select
        carrier_flight_num
        ,flight_rk
        ,dt

        ,origin_airport_dk
        ,dest_airport_dk

        ,carrier_code
        ,tail_num
        ,distance
        ,dep_delay_min
        ,cancel_flg

        --,delay_reason  as delay_reason_orig

        ,case
            when dep_delay_min > 0 and delay_reason != ''
                then
                    delay_reason
            when dep_delay_min > 0 and delay_reason = ''
                then
                    'неизвестно'
            else
                null
        end  as delay_reason
        ,now()  as load_dt
    from
        wt_flight
)

select
    count(1)
from
    wt_fin
where 1=1

