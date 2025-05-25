with wt_table as (
    select
         "Invoice"      as invoice
        ,"StockCode"    as scode
        ,"Description"  as descr
        ,"Quantity"     as quantity
        ,to_timestamp("InvoiceDate", 'dd.mm.yyyy hh24:mi')  as invoice_dt
        ,replace("Price", ',', '.')::numeric  as price
        ,"Customer ID"  as cust_id
        ,"Country"      as country
    from
        online_retail_listing
    where 1=1
        and "Customer ID" is not null
)

,wt_max_dt as (
    select
        max(invoice_dt)  as max_invoice_dt
    from
        wt_table
)


,wt_cust_prep as (
    select
        a.cust_id
        ,extract(epoch from b.max_invoice_dt - max(a.invoice_dt)) / 60 / 60 / 24  as last_purchase_day_diff
        ,count(distinct invoice)  as invoice_cnt
        ,sum(quantity * price)  as purchase_amt
    from
        wt_table  a
        cross join wt_max_dt  b
    where 1=1
    group by
        a.cust_id
        ,b.max_invoice_dt
)

,wt_fin as (
    select
        cust_id
        ,last_purchase_day_diff
        ,invoice_cnt
        ,purchase_amt
        ,ntile(5) over (order by last_purchase_day_diff desc)  as r_score
        ,ntile(5) over (order by invoice_cnt                )  as f_score
        ,ntile(5) over (order by purchase_amt               )  as m_score
    from
        wt_cust_prep
    where 1=1
)

select
    concat(
    	 r_score
    	,f_score
    	,m_score
    )  as score
    ,count(1)  as cust_cnt
from
    wt_fin
where 1=1
group by
    concat(
         r_score
        ,f_score
        ,m_score
    )
