-- 1
with wt_mn_list as (
    -- get a list of months from order dates
    select distinct
        date_trunc('month', orderdate)  as mn
    from
        salesorderheader
    order by
        date_trunc('month', orderdate)
)


,wt_define_continuous_period as (
    -- define a group_id for the period's begin and end
    select
         mn
        ,mn - interval '1 month' * row_number() over (order by mn)  as group_id
    from
        wt_mn_list
)


,wt_get_a_period_w_duration_of_3_months as (
    select
         min(mn)  as from_dt
        ,max(mn)  as to_dt
    from
        wt_define_continuous_period
    group by
        group_id
    having 1=1
        and extract(epoch from max(mn) - min(mn)) / 60 / 60 / 24 > 92
)


,wt_par as (
    -- get any 3 months period to use it as a filter
    select
         from_dt::date  as from_dt
        ,(from_dt + interval '3 months')::date - 1  as to_dt
    from
        wt_get_a_period_w_duration_of_3_months
    limit 1
)


,wt_stats as (
    select
         date_trunc('month', orderdate)::date  as mn
        ,pc1.name  as product_category_name
        ,count(distinct sh1.salesorderid)  as order_cnt
    from
        salesorderheader  sh1

        join wt_par  pa1
            on  sh1.orderdate >= pa1.from_dt
            and sh1.orderdate <  pa1.to_dt + 1

        join salesorderdetail  sd1
            on sd1.salesorderid = sh1.salesorderid

        join product  pr1
            on pr1.productid = sd1.productid

        join productsubcategory  psc1
            on psc1.productsubcategoryid = pr1.productsubcategoryid

        join productcategory  pc1
            on pc1.productcategoryid = psc1.productcategoryid
    where 1=1
    group by
         date_trunc('month', orderdate)
        ,pc1.name
)


select
     extract(year from mn)  as order_year
    ,extract(month from mn)  as order_month

    ,product_category_name
    ,order_cnt
    ,order_cnt - lag(order_cnt, 1, order_cnt) over (order by mn)  as order_cnt_diff_from_prev_month

    ,current_timestamp
from
    wt_stats
where 1=1
order by
     mn
    ,product_category_name


-- 2

with wt_par as (
    select
         date'2013-01-01'  as from_dt
        ,date'2013-12-31'  as to_dt
    limit 1
)

,wt_product_amt as (
    select
         pr1.name  as product_name
        ,sum(sd1.linetotal)  as amt
        ,dense_rank() over (order by sum(sd1.linetotal) desc)  as rn1
    from
        salesorderheader  sh1

        join wt_par  pa1
            on  sh1.orderdate >= pa1.from_dt
            and sh1.orderdate <  pa1.to_dt + 1

        join salesorderdetail  sd1
            on sd1.salesorderid = sh1.salesorderid

        join product  pr1
            on pr1.productid = sd1.productid
    where 1=1
    group by
        pr1.name
)

,wt_total_amt as (
    select
         product_name
        ,amt
        ,sum(amt) over ()  as total_amt
        ,rn1
    from
        wt_product_amt
    where 1=1
)

select
     product_name
    ,amt
    -- "case when total_amt = 0 then 0 else amt / total_amt end" in other way
    ,round(coalesce(amt / nullif(total_amt, 0), 0) * 100, 2)  as proc_to_total_amt

    ,current_timestamp
from
    wt_total_amt
where 1=1
    and rn1 <= 10
order by
    rn1


-- 3

with wt_par as (
    select
         date'2013-05-01'  as from_dt
        ,date'2013-05-31'  as to_dt
    limit 1
)

,wt_all_data as (
    select
        -- sh1.salesordernumber  -- is null everywhere so I use salesorderid
         --coalesce(sh1.purchaseordernumber, 'none')  as order_num
         sh1.salesorderid  as order_num
        ,pr1.name  as product_name
        ,ad1.city  as delivery_city
        ,sh1.totaldue
        ,pr1.productnumber
        ,sum(sh1.totaldue) over (partition by ad1.city)  as city_totaldue
    from
        salesorderheader  sh1

        join wt_par  pa1
            on  sh1.orderdate >= pa1.from_dt
            and sh1.orderdate <  pa1.to_dt + 1

        join salesorderdetail  sd1
            on sd1.salesorderid = sh1.salesorderid

        join product  pr1
            on pr1.productid = sd1.productid

    --    left join customeraddress  ca1
    --        on ca1.customerid = sh1.customerid

        left join address  ad1
            --on ad1.addressid = ca1.addressid
            on ad1.addressid = sh1.shiptoaddressid
    where 1=1
)

select
     order_num
    ,count(distinct product_name)  as uniq_product_cnt
    ,delivery_city

    ,round(coalesce(sum(totaldue) / nullif(city_totaldue, 0), 0), 3)  as totaldue_city_rate
    ,(array_agg(product_name order by totaldue desc))[1]  as max_totaldue_product
    ,string_agg(productnumber, ', ')  as productnumber_list

    ,current_timestamp
from
    wt_all_data
where 1=1
group by
    order_num
    ,delivery_city
    ,city_totaldue


-- 4

with wt_par as (
    select
         date'2013-01-01'  as from_dt
        ,date'2013-12-31'  as to_dt
        ,0.8   as sa_rate
        ,0.95  as sb_rate
    limit 1
)


,wt_product_amt as (
    select
         pr1.name  as product_name
        ,sum(sd1.linetotal)  as amt

        ,pa1.sa_rate
        ,pa1.sb_rate
    from
        salesorderheader  sh1

        join wt_par  pa1
            on  sh1.orderdate >= pa1.from_dt
            and sh1.orderdate <  pa1.to_dt + 1

        join salesorderdetail  sd1
            on sd1.salesorderid = sh1.salesorderid

        join product  pr1
            on pr1.productid = sd1.productid
    where 1=1
    group by
         pr1.name
        ,pa1.sa_rate
        ,pa1.sb_rate
)


,wt_total_amt as (
    select
         product_name
        ,amt
        ,sum(amt) over ()  as s

        ,sa_rate
        ,sb_rate
    from
        wt_product_amt
)


,wt_srti as (
    select
         product_name
        ,amt
        ,s * sa_rate  as sa_limit
        ,s * sb_rate  as sb_limit
        ,sum(amt) over (order by amt desc rows between unbounded preceding and current row)  as srti
    from
        wt_total_amt
)


select
     product_name
    ,amt
    ,case
        when srti <= sa_limit then 'A'
        when srti <= sb_limit then 'B'
        else 'C'
     end  as product_class

    ,current_timestamp
from
    wt_srti
order by
    amt desc



-- 5

with wt_par as (
    select
         date'2013-05-01'  as from_dt
        ,date'2013-05-31'  as to_dt
    limit 1
)

,wt_amt_for_dt as (
    select
         sh1.orderdate::date  as dt
        ,sum(sh1.totaldue)  as amt
    from
        salesorderheader  sh1
        join wt_par  pa1
            on  sh1.orderdate >= pa1.from_dt
            and sh1.orderdate <  pa1.to_dt + 1

    where 1=1
    group by
        sh1.orderdate::date
)

,wt_ma_5_days as (
    select
         dt
        ,amt
        ,avg(amt) over (order by dt rows between 4 preceding and current row)  as ma_5_days

        ,amt * 5  as t_amt
        ,lag(amt, 1, 0) over (order by dt) * 4  as t_minus_1_amt
        ,lag(amt, 2, 0) over (order by dt) * 3  as t_minus_2_amt
        ,lag(amt, 3, 0) over (order by dt) * 2  as t_minus_3_amt
        ,lag(amt, 4, 0) over (order by dt) * 1  as t_minus_4_amt
    from
        wt_amt_for_dt
)

select
     dt
    ,ma_5_days
    ,t_amt + t_minus_1_amt + t_minus_2_amt + t_minus_3_amt + t_minus_4_amt  as weighted_ma_5_days
    ,current_timestamp
from
    wt_ma_5_days
where 1=1
order by
    dt





-- 6

with wt_par as (
    select
         date'2017-02-01'  as from_dt
        ,date'2017-02-01'  as to_dt
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
    limit 1
)


,wt_modify_flight_tz as (
    select
         flight_id
        ,actual_departure  as depart_dt
        ,actual_arrival    as arrive_dt
    from
        flights  a
        join wt_par  pa1
            on  a.actual_departure at time zone pa1.msc_tz >= pa1.from_dt
            and a.actual_departure at time zone pa1.msc_tz <  pa1.to_dt + 1
    where 1=1
)


,wt_start_stop_dt as (
    select distinct
         depart_dt  as start_dt
        ,arrive_dt  as stop_dt
    from
        wt_modify_flight_tz
    where 1=1
)


,wt_flight_in_air as (
    select
         ss.start_dt
        ,ss.stop_dt
        ,count(1)  as flight_cnt
    from
        wt_modify_flight_tz  a

        join wt_start_stop_dt  ss
            on  ss.start_dt <= a.arrive_dt
            and ss.stop_dt  >= a.depart_dt
    where 1=1
    group by
         ss.start_dt
        ,ss.stop_dt
)


,wt_flight_arrived_till_start_dt as (
    select
         ss.start_dt
        ,ss.stop_dt
        ,count(1)  as flight_cnt
    from
        wt_modify_flight_tz  a

        join wt_start_stop_dt  ss
            on  ss.start_dt > a.arrive_dt
    where 1=1
    group by
         ss.start_dt
        ,ss.stop_dt
)


select
     coalesce(air1.start_dt, fin1.start_dt)  as start_dttm
    ,coalesce(air1.stop_dt,  fin1.stop_dt )  as end_dttm

    ,coalesce(air1.flight_cnt, 0)  as flights_air_cnt
    ,coalesce(fin1.flight_cnt, 0)  as flights_finished_cnt

    ,current_timestamp
from
    wt_flight_in_air  air1
    full join wt_flight_arrived_till_start_dt  fin1
        on  fin1.start_dt = air1.start_dt
        and fin1.stop_dt  = air1.stop_dt
where 1=1
order by
     coalesce(air1.start_dt, fin1.start_dt)
    ,coalesce(air1.stop_dt,  fin1.stop_dt )
