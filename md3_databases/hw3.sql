-- 1.

with wt_dir_tz_and_tm as (
    select
         a.departure_airport || ' - ' || a.arrival_airport  as direction
        ,(a.actual_departure at time zone coalesce(b.timezone, 'utc'))::time  as tm
    from
        flights  a
        left join airports_data  b
            on b.airport_code = a.departure_airport
    where 1=1
        and a.actual_departure is not null
        and a.actual_departure at time zone coalesce(b.timezone, 'utc') >= date'2017-03-01'
        and a.actual_departure at time zone coalesce(b.timezone, 'utc') <  date'2017-04-01'
)

,wt_dir_fli_cnt as (
    select
         direction
        ,sum(case when tm >= '21:00:00' or  tm <= '1:00:00' then 1 else 0 end)  as night_fli_cnt
        ,sum(case when tm >= '6:00:00'  and tm <= '9:00:00' then 1 else 0 end)  as day_fli_cnt
    from
        wt_dir_tz_and_tm
    where 1=1
    group by
        direction
)

,wt_rank as (
    select
         direction
        ,night_fli_cnt
        ,day_fli_cnt
        ,dense_rank() over (order by night_fli_cnt desc)  as night_rn
        ,dense_rank() over (order by day_fli_cnt   desc)  as day_rn
    from
        wt_dir_fli_cnt
)

select
    -- there can be several champions, that is why I use rank and string_agg
     sum(night_fli_cnt)                                         as night_flight_cnt
    ,string_agg(direction, ' :: ') filter (where night_rn = 1)  as night_champion_directions
    ,sum(day_fli_cnt)                                           as day_flight_cnt
    ,string_agg(direction, ' :: ') filter (where day_rn = 1)    as day_champion_directions
    ,current_timestamp
from
    wt_rank
where 1=1




-- 2

with wt_msc_tz as (
    select
        timezone  as tz
    from
        airports_data
    where 1=1
        and city ->> 'ru' = 'Москва'
        and airport_code = 'DME'
    limit 1
)

,wt_departure as (
    -- the subselects are faster here than join and filter
    select
         a.flight_id
        ,(a.actual_departure at time zone (select tz from wt_msc_tz))::date  as departure_dt
        --,(a.actual_departure at time zone b.tz)::date  as departure_dt
    from
        flights  a
        --join wt_msc_tz  b
        --    on  a.actual_departure at time zone b.tz >= date'2017-03-01'
        --    and a.actual_departure at time zone b.tz <  date'2017-04-01'
    where 1=1
        and a.actual_departure at time zone (select tz from wt_msc_tz) >= date'2017-03-01'
        and a.actual_departure at time zone (select tz from wt_msc_tz) <  date'2017-04-01'
)

select
     a.departure_dt
    ,b.fare_conditions
    ,sum(b.amount)  as amt
    ,current_timestamp
from
    wt_departure  a

    join ticket_flights  b
        on b.flight_id = a.flight_id
where 1=1
group by
     a.departure_dt
    ,b.fare_conditions
order by
     a.departure_dt
    ,b.fare_conditions




-- 3

with wt_subcat as (
    select
         a.salesorderid
        ,a.productid
        ,c.productsubcategoryid
        ,c.name  as subcat_name

        ,count(1) over (partition by a.salesorderid, c.name)  as sub_cnt
        ,sum(case when c.name = 'Wheels' then 1 else 0 end) over (partition by a.salesorderid)  as wheel_cnt
    from
        salesorderdetail  a

        join product  b
            on b.productid = a.productid

        join productsubcategory  c
            on c.productsubcategoryid = b.productsubcategoryid
    where 1=1
--        and a.salesorderid = 46932
)


,wt_rank as (
    select
         productsubcategoryid
        ,subcat_name
        ,count(distinct salesorderid)  as order_cnt
        ,dense_rank() over (order by count(distinct salesorderid) desc)  as rank1
    from
        wt_subcat
    where 1=1
    group by
         productsubcategoryid
        ,subcat_name
)

select
     productsubcategoryid
    ,subcat_name
    ,order_cnt
    ,current_timestamp
from
    wt_rank
where 1=1
    and rank1 <= 5
order by
    rank1



-- 4
select
     left(name, 1)  as first_letter
    ,count(1)
    ,current_timestamp
from
    country  a
group by
    left(name, 1)
order by
    left(name, 1)




-- 5

select
     count(distinct a.name)  as country_cnt
    ,current_timestamp
from
    country  a
    join countrylanguage  b
        on  b.countrycode = a.code
        and b.isofficial is true
        and b.language in ('English', 'German')
where 1=1



-- 6

with wt_avg_le as (
    select
         region
        ,name  as country_name
        ,lifeexpectancy
        ,avg(lifeexpectancy) over (partition by region)  as avg_reg_le
    from
        country
    where 1=1
)

select
     region
    ,country_name
    ,lifeexpectancy
    ,current_timestamp
from
    wt_avg_le
where 1=1
    and lifeexpectancy > avg_reg_le


-- 7

with wt_asia_and_africa as (
    select
         array_agg(b.code) filter (where b.continent = 'Asia'  )  as asia_code_arr
        ,array_agg(b.code) filter (where b.continent = 'Africa')  as africa_code_arr
    from
        country  b
    where 1=1
        and b.continent in ('Asia', 'Africa')
)

select
     a.language
    ,current_timestamp
from
    countrylanguage  a
    cross join wt_asia_and_africa  b
where 1=1
group by
     a.language
    ,b.asia_code_arr
    ,b.africa_code_arr
having 1=1
    and array_agg(a.countrycode) && b.asia_code_arr
    and array_agg(a.countrycode) && b.africa_code_arr
order by
    a.language




-- 8

with wt_raw as (
    select
         to_char(sh.orderdate, 'Month')  as month_name
        ,to_char(sh.orderdate, 'mm')     as mm
        ,to_char(sh.orderdate, 'yyyy')   as yyyy

        ,c.name  as cat_name
        ,a.linetotal
    from
        salesorderdetail  a

        join salesorderheader  sh
            on  sh.salesorderid = a.salesorderid
            and sh.orderdate >= date'2012-01-01'
            and sh.orderdate <  date'2014-01-01'

        join product  b
            on b.productid = a.productid

        join productsubcategory  sc
            on sc.productsubcategoryid = b.productsubcategoryid

        join productcategory  c
            on c.productcategoryid = sc.productcategoryid
    where 1=1
)

,wt_year_by_col as (
    select
         month_name
        ,mm
        ,cat_name

        ,sum(case when yyyy = '2012' then linetotal else 0 end)  as linetotal_2012
        ,sum(case when yyyy = '2013' then linetotal else 0 end)  as linetotal_2013
    from
        wt_raw
    where 1=1
    group by
         month_name
        ,cat_name
        ,mm
)

select
     month_name
    ,cat_name
    ,linetotal_2012
    ,linetotal_2013

    ,round(
        case
            when linetotal_2012 = 0
                then
                    1
            else
                (linetotal_2013 - linetotal_2012) / linetotal_2012 * 100
        end
        ,2
    )  as diff_in_prc

    ,current_timestamp
from
    wt_year_by_col
where 1=1
order by
     mm
    ,cat_name



-- 9

with wt_par as materialized (
    select
         date'2017-03-01'  as from_dt
        ,date'2017-03-31'  as to_dt
        ,(
            select
                timezone  as tz
            from
                airports_data
            where 1=1
                and city ->> 'ru' = 'Москва'
                and airport_code = 'DME'
            limit 1
        )  as msc_tz
)


,wt_departure_and_arrival as (
    select
         a.flight_id

        ,a.departure_airport                                 as airport
        ,'departure'                                         as typ
        ,(a.actual_departure at time zone pa1.msc_tz)::date  as dt
    from
        flights  a
        join wt_par  pa1
            on  a.actual_departure at time zone pa1.msc_tz >= pa1.from_dt
            and a.actual_departure at time zone pa1.msc_tz <  pa1.to_dt + 1
    where 1=1

    union all

    select
         a.flight_id

        ,a.arrival_airport
        ,'arrival'  as typ
        ,(a.actual_arrival at time zone pa1.msc_tz)::date  as dt
    from
        flights  a
        join wt_par  pa1
            on  a.actual_arrival at time zone pa1.msc_tz >= pa1.from_dt
            and a.actual_arrival at time zone pa1.msc_tz <  pa1.to_dt + 1
    where 1=1
)


,wt_ticket_cnt as (
    select
         flight_id
        ,count(1)  as ticket_cnt
    from
        ticket_flights
    where 1=1
        and flight_id in (select flight_id from wt_departure_and_arrival)
    group by
        flight_id
)


,wt_rank as (
    select
         coalesce(b.airport_name ->> 'ru', a.airport)  as airport_name_ru

        ,sum(case when a.typ = 'departure' then 1 else 0 end)  as departure_cnt
        ,sum(case when a.typ = 'arrival'   then 1 else 0 end)  as arrival_cnt
        ,count(1)                                              as flight_cnt
        ,sum(coalesce(c.ticket_cnt, 0))                        as ticket_cnt

        ,dense_rank() over (order by count(1) desc)  as rank1
    from
        wt_departure_and_arrival  a

        left join airports_data  b
            on b.airport_code = a.airport

        left join wt_ticket_cnt  c
            on c.flight_id = a.flight_id
    where 1=1
    group by
         coalesce(b.airport_name ->> 'ru', a.airport)
)

select
     airport_name_ru

    ,departure_cnt
    ,arrival_cnt
    ,flight_cnt
    ,ticket_cnt

    ,current_timestamp
from
    wt_rank
where 1=1
    and rank1 <= 10

