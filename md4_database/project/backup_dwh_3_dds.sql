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
    ,hash       text
    ,load_dt    timestamp  -- Время загрузки Время data interval end
)
;


--drop table if exists dds.flight
--;
create table if not exists dds.flight (
     id                  text
    ,carrier_code        text
    ,carrier_flight_num  text
    ,tail_num            text
    ,orig_airport_id     text
    ,dt                  date
    ,cancel_flg          int2
    ,dest_airport_id     text
    ,cancel_cd           text
    ,origin_city_name    text
    ,dest_city_name      text
    ,dep_delay_min       int4
    ,dep_delay_group_num numeric
    ,arr_delay_min       int4
    ,arr_delay_group_num numeric
    ,flights_cnt         int4
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

        -- ,md5(  -- doesn't work - two equal values for different rows !!!
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
    -- we add all columns because of the Inmon architecture
    select
         stg1.carrier_code
        ,stg1.carrier_flight_num
        ,stg1.tail_num
        ,stg1.cancelled_flg  as cancel_flg
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

        ,stg1.origin_code
        ,stg1.origin_city_name
        ,stg1.dest_code
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

        join wt_par  pa1
            on stg1.cancelled_flg = any(pa1.cancel_flg)

        join ods.airport  orig_airport1
            on orig_airport1.iata_code = stg1.origin_code

        join ods.airport  dest_airport1
            on dest_airport1.iata_code = stg1.dest_code

        left join ods.airport_tz  orig_airport_tz1
            on orig_airport_tz1.iata_code = orig_airport1.iata_code

        left join ods.airport_tz  dest_airport_tz1
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
        ,origin_city_name
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
        || '.' || coalesce(dt                 :: text, '0')
        as id

        ,carrier_code
        ,carrier_flight_num
        ,tail_num
        ,orig_airport_id
        ,dt

        ,cancel_flg
        ,dest_airport_id
        ,cancel_cd
        ,origin_city_name
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
            ,coalesce(origin_city_name    :: text, '')
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



--drop table if exists dds.flight_successful
--;
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
    ,constraint flight_successful_pk primary key(carrier_flight_num, flight_dttm_local, origin_airport_dk)
)
;



--drop table if exists dds.flight_cancelled
--;
create table if not exists dds.flight_cancelled (
     carrier_flight_num text        -- Номер рейса перевозчика
    ,sched_dttm_local   timestamp   -- Локальное время и дата вылета
    ,origin_airport_dk  text        -- Код аэропорта отправления
    ,dest_airport_dk    text        -- Код аэропорта назначения
    ,carrier_code       text        -- Код перевозчика
    ,cancellation_code  text        -- Код причины отмены рейса
    ,hash               text
    ,processed_dttm     timestamp   -- Время вставки записи
    ,constraint flight_cancelled_pk primary key(carrier_flight_num, sched_dttm_local, origin_airport_dk)
)
;



with wt_dds_flight_succ as (
    select
        -- PK
         carrier_flight_num
        ,actual_dep_dt    as flight_dttm_local
        ,orig_airport_id  as origin_airport_dk


        ,array_agg(dest_airport_id  )  as dest_airport_dk
        ,array_agg(carrier_code     )  as carrier_code
        ,array_agg(tail_num         )  as tail_num
        ,array_agg(distance         )  as distance_m
        ,array_agg(dep_delay_min    )  as dep_delay_min
        ,array_agg(delay_reason_list)  as delay_reasons_code

        ,localtimestamp  as load_dt
    from
        dds.flight
    where 1=1
        and cancel_flg = 0
    group by
         carrier_flight_num
        ,actual_dep_dt
        ,orig_airport_id

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
            (tar1.hash is not null and tar1.hash != sou1.hash)
            or tar1.hash is null
        )
)
--select * from wt_delete_and_insert



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
