create schema if not exists ods
;


drop table if exists ods.flight
;
create table if not exists ods.flight (
     year                       text
    ,month                      text
    ,flight_dt                  text
    ,carrier_code               text
    ,tail_num                   text
    ,carrier_flight_num         text
    ,origin_code                text
    ,origin_city_name           text
    ,dest_code                  text
    ,dest_city_name             text
    ,scheduled_dep_tm           text
    ,actual_dep_tm              text
    ,dep_delay_min              text
    ,dep_delay_group_num        text
    ,wheels_off_tm              text
    ,wheels_on_tm               text
    ,scheduled_arr_tm           text
    ,actual_arr_tm              text
    ,arr_delay_min              text
    ,arr_delay_group_num        text
    ,cancelled_flg              text
    ,cancellation_code          text
    ,flights_cnt                text
    ,distance                   text
    ,distance_group_num         text
    ,carrier_delay_min          text
    ,weather_delay_min          text
    ,nas_delay_min              text
    ,security_delay_min         text
    ,late_aircraft_delay_min    text
)
;


drop table if exists ods.airport
;
create table if not exists ods.airport (
	 id                 text
	,ident              text
	,type               text
	,name               text
	,latitude_deg       text
	,longitude_deg      text
	,elevation_ft       text
	,continent          text
	,iso_country        text
	,iso_region         text
	,municipality       text
	,scheduled_service  text
	,icao_code          text
	,iata_code          text
	,gps_code           text
	,local_code         text
	,home_link          text
	,wikipedia_link     text
	,keywords           text
)
;
--create index if not exists ix_ods_airport_icao on ods.airport using btree (icao_code)
--;


drop table if exists ods.weather
;
create table ods.weather (
	 icao_code   text
	,local_time  text
	,raw_t       text
	,raw_p0      text
	,raw_p       text
	,raw_u       text
	,raw_dd      text
	,raw_ff      text
	,raw_ff10    text
	,raw_ww      text
	,raw_w_w_    text
	,raw_c       text
	,raw_vv      text
	,raw_td      text
)
;
