-- dds.flight definition

-- Drop table

-- DROP TABLE dds.flight;

create table dds.flight (
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
);

-- Permissions

alter table dds.flight OWNER to dwh_2024_s004;
grant
all
on table dds.flight to dwh_2024_s004;