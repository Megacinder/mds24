/*
select
    string_agg(column_name, ' || ' order by ordinal_position)  as col_list
from
    information_schema.columns
where 1=1
    and table_name = 'weather'
    and column_name != 'local_time'
*/

explain analyze
with wt_hash as (
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
        ,md5(raw_t || raw_p0 || raw_p || raw_u || raw_dd || raw_ff || raw_ff10 || raw_ww || raw_w_w_ || raw_c || raw_vv || raw_td)  as hash
    from
        ods.weather
    where 1=1
)

,wt_prev_hash as (
    select
        icao_code
        ,dt
        ,hash
        ,lag(hash, 1, hash) over (partition by icao_code order by dt)  as prev_hash
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
        ,sum(change_flg) over (partition by icao_code order by dt, hash)  as group_key
    from
        wt_change_flg
    where 1=1
)

-- ad56894da5e8d0851da83e4f3287b8c4
,wt_valid_period as (
    select
        icao_code
        ,hash
        ,min(dt)  as valid_from
        ,lead(
            min(dt)
            ,1
            ,'5999-01-01'::timestamp
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
        and icao_code = 'KFLG'
        and dt between date'2024-12-01' and date'2024-12-10'
    group by
        icao_code
        ,hash
        ,group_key
)

,wt_distinct_values as (
    select distinct  -- unique hash and his values
        hash
        ,icao_code
        ,temperatu_celc_deegree
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
        wt_hash
)

select
    a.icao_code
    ,a.valid_from
    ,a.valid_to
    ,b.temperatu_celc_deegree
    ,b.pressure_station_merc_mlm
    ,b.pressure_see_level_merc_mlm
    ,b.humidity_prc
    ,b.wind_direction
    ,b.wind_speed_meters_per_sec
    ,b.max_gust_10m_meters_per_sec
    ,b.special_present_weather_phenomena
    ,b.recent_weather_phenomena_operational
    ,b.cloud_cover
    ,b.horizontal_visibility_km
    ,b.dewpoint_temperature_celc_deegree
from
    wt_valid_period  a
    join wt_distinct_values  b
        on  b.icao_code = a.icao_code
        and b.hash      = a.hash
where 1=1

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