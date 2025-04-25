with wt_par as (
    select
        date'2017-03-01'  as dt
)

,wt_hour as (
    select
         hh  as from_hh
        --,hh + interval '1 hour' - interval '1 ms'  as to_hh
         ,hh + interval '1 hour'  as to_hh
    from
        wt_par  pa1
        join lateral generate_series(
             pa1.dt
            ,pa1.dt + 1
            ,interval '1 hour'
        )  as hh on true
    where 1=1
        and hh::date = pa1.dt
)

select * from wt_hour

,wt_fli as (
    select
         flight_id
        ,status
        ,actual_departure  as depart_dt
        ,actual_arrival    as arrive_dt
    from
        flights  a
        join wt_par  pa1
            on  a.actual_departure at time zone 'utc' >= pa1.dt
            and a.actual_departure at time zone 'utc' <  pa1.dt + 1
)


,wt_prep as (
    select
        a.to_hh
        ,sum(case when b.status != 'Cancelled' then 1 else 0 end)  as fli_cnt
        ,sum(case when b.status  = 'Cancelled' then 1 else 0 end)  as cancel_cnt
    from
        wt_hour  a
        left join wt_fli  b
            on  b.depart_dt >= a.from_hh
            and b.depart_dt <  a.to_hh
    group by
        a.to_hh
)

select
     a.to_hh
    ,sum(fli_cnt) over (order by a.to_hh rows between unbounded preceding and current row)  as cum_sum_fli_cnt
    ,fli_cnt
    ,sum(cancel_cnt) over (order by a.to_hh rows between unbounded preceding and current row)  as cum_sum_cancel_cnt
    ,'Ahremenko Mihail'  as my_name
    ,user
from
    wt_prep  a
