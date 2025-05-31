create schema if not exists dm;


drop table if exists dm.airport_weather
;
create table if not exists dm.airport_weather (
     rain_flg           smallint  --  Дождь w_phenomena включает 'rain' или ws_phenomena включает 'rain'
    ,snow_flg           smallint  --  Снег w_phenomena включает 'snow' или ws_phenomena включает 'snow'
    ,thunderstorm_flg   smallint  --  Гроза w_phenomena включает 'thunderstorm' или ws_phenomena включает 'thunderstorm'
    ,fog_mist_flg       smallint  --  Туман/Мгла w_phenomena включает 'fog' или 'mist' или ws_phenomena включает 'fog' или 'mist'
    ,drizzle_flg        smallint  --  Морось w_phenomena включает 'drizzle' или ws_phenomena включает 'drizzle'
    ,freezing_flg       smallint  --  Холодно t < 0
    ,w_speed            numeric  --  Скорость ветра Значение из поля w_speed
    ,max_gws            numeric  --  Макс. порывы ветра Значение из поля max_gws
    ,t_deg              numeric  --  Температура Значение из поля T
    ,airport_rk         text  --  ID аэропорта вылета Подставляется из dds.airport по известному ICAO коду (icao_code)
    ,flights_cnt        int  --  Количество вылетевших рейсов
    ,delay_min_avg      numeric  --  Средняя задержка вылета
    ,valid_from_dttm    timestamp  --  Начало периода Значение из поля loctime
    ,valid_to_dttm      timestamp  --  Окончание периода Следующее известное время сбора данных для этого аэропорта или дата в будущем 5999-01-01
    --,processed_dttm     int  --  Время загрузки Время data interval end
    ,load_dt            timestamp  --  Время загрузки Время data interval end
    ,constraint dm_airport_weather_pk primary key (airport_rk, valid_from_dttm)
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

insert into dm.airport_weather
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
