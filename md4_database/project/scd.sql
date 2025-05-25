--explain analyze
with wt_par as (
    select
--        '{KFLG}' :: text[]  as airport_arr
--        ,date'2024-12-01'   as from_dt
--        ,date'2024-12-07'   as to_dt

         null :: text[]  as airport_arr
        ,null :: date  as from_dt
        ,null :: date  as to_dt
)


,wt_stg_table as (
    select  -- choose only dds values
--         stg1.icao_code
         1  as priority
        ,airport1.id :: text || '.' || stg1.icao_code  as airport_rk
        ,stg1.dt

        ,stg1.wind_speed_meters_per_sec    as w_speed
        ,stg1.max_gust_10m_meters_per_sec  as max_gws
        ,stg1.temperature_celc_deegree     as t_deg

--        ,md5(  -- doesnt' work - two equal values for different rows !!!
--               coalesce(stg1.wind_speed_meters_per_sec   :: text, '')
--            || coalesce(stg1.max_gust_10m_meters_per_sec :: text, '')
--            || coalesce(stg1.temperature_celc_deegree    :: text, '')
--        )  as hash

        ,concat_ws(
             '::'
            ,coalesce(stg1.wind_speed_meters_per_sec   :: text, '')
            ,coalesce(stg1.max_gust_10m_meters_per_sec :: text, '')
            ,coalesce(stg1.temperature_celc_deegree    :: text, '')
        )  as hash
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
         2  as priority
        ,airport_rk
        ,valid_from  as dt

        ,w_speed
        ,max_gws
        ,t_deg

--        ,md5(  -- doesnt' work - two equal values for different rows !!!
--               coalesce(w_speed :: text, '')
--            || coalesce(max_gws :: text, '')
--            || coalesce(t_deg   :: text, '')
--        )  as hash
        ,concat_ws(
             '::'
            ,coalesce(w_speed :: text, '')
            ,coalesce(max_gws :: text, '')
            ,coalesce(t_deg   :: text, '')
        )  as hash
    from
        dds.weather
    where 1=1
)


,wt_stg_table_wo_dds as (
    select
        *
    from
        wt_stg_table

    union all

    select
        *
    from
        wt_dds_table
    where 1=1
)

--select airport_rk, dt, count(1) from wt_stg_table_wo_dds group by airport_rk, dt having count(1) > 1

--select
--    *
--from
--    wt_norm_col_name_w_hash
--where 1=1
--    and airport_rk = '3530.KFLG'
--    and dt = '2024-01-15 20:57:00.000'


,wt_old_period_priority as (
    -- if data has changed in previous period, then we choose new values instead
    select
        airport_rk
        ,dt
        ,(array_agg(w_speed order by priority))[1]  as w_speed
        ,(array_agg(max_gws order by priority))[1]  as max_gws
        ,(array_agg(t_deg   order by priority))[1]  as t_deg
        ,(array_agg(hash    order by priority))[1]  as hash
        ,count(1)  as array_len
    from
        wt_stg_table_wo_dds
    group by
        airport_rk
        ,dt
)

--select * from wt_old_period_priority order by airport_rk, dt
--where airport_rk = '3530.KFLG'
--    and dt = '2024-12-09 23:57:00.000'


--select airport_rk, dt, count(1) from wt_old_period_priority group by airport_rk, dt having count(1) > 1

--select * from wt_stg_table

,wt_prev_hash as (
    select
        airport_rk
        ,dt
        ,hash
        ,lag(hash, 1, hash) over (partition by airport_rk order by dt)  as prev_hash
    from
        wt_old_period_priority
    where 1=1
)

--select * from wt_prev_hash
--select * from wt_prev_hash where airport_rk = '3530.KFLG' and dt = '2024-01-15 20:57:00.000'

,wt_change_flg as (
    select
        *
        ,case when hash != prev_hash then 1 else 0 end  as change_flg
    from
        wt_prev_hash
    where 1=1
)

--select * from wt_change_flg where airport_rk = '3530.KFLG' and dt = '2024-01-15 20:57:00.000'

,wt_group_key as (
    select
        *
        ,sum(change_flg) over (partition by airport_rk order by dt, hash)  as group_key
    from
        wt_change_flg
    where 1=1
)

--select * from wt_group_key where airport_rk = '3530.KFLG' and dt = '2024-01-15 20:57:00.000'

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
    from
        wt_group_key
    where 1=1
        --and group_key = '7050'
    group by
        airport_rk
        ,hash
        ,group_key
)

--select * from wt_valid_period where airport_rk = '3530.KFLG' and valid_from = '2024-01-15 20:57:00.000'

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

--select * from wt_distinct_values where airport_rk = '3530.KFLG' and hash = '2::::0.0'  --and dt = '2024-01-15 20:57:00.000'


,wt_fin as (
    select
         period1.airport_rk

        ,values1.w_speed
        ,values1.max_gws
        ,values1.t_deg

        ,period1.valid_from
        ,period1.valid_to
        ,localtimestamp  as load_dt
    --    ,period1.collapsed_row_cnt
    from
        wt_valid_period  period1

        join wt_distinct_values  values1
            on  values1.airport_rk = period1.airport_rk
            and values1.hash       = period1.hash
    where 1=1
--        and period1.airport_rk = '3530.KFLG'
--        and period1.valid_from = '2024-01-15 20:57:00.000'
)


--select * from wt_fin

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


--insert into dds.weather
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
--select * from wt_delete


--select
--    stg1.*
--from
--    wt_fin  stg1
----    left join dds.weather  dds1
----        on  dds1.airport_rk = stg1.airport_rk
----        and dds1.valid_from = stg1.valid_from
----        and dds1.valid_to   = stg1.valid_to
--where 1=1
----    and dds1.airport_rk is null
insert into dds.weather
select
     airport_rk

    ,w_speed
    ,max_gws
    ,t_deg

    ,valid_from
    ,valid_to
from
    wt_fin
where 1=1

--truncate table dds.weather

