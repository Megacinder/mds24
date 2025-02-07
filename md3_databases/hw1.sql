-- { task 1 begin
drop database if exists dc1_hw1;
create database dc1_hw1;
;
-- make the database above as a default - PG doesn't have a cross-database access

drop table if exists app_user    cascade;
drop table if exists trip        cascade;
drop table if exists trip_cities cascade;
drop table if exists city        cascade;


create table app_user (
     user_id    int         generated always as identity primary key
    ,user_name  varchar(80) not null
    ,created_at timestamptz not null default now()
)
;


create table trip (
     trip_id    int          not null generated always as identity primary key
    ,trip_name  varchar(500) not null
    ,start_date date         null
    ,user_id    int          not null
)
;


create table trip_cities (
     trip_id        int not null
    ,ord_number     int not null
    ,city_id        int not null
    ,stay_duration  int null
    ,primary key (trip_id, ord_number)
)
;


create table city (
     city_id        int         not null generated always as identity primary key
    ,city_name      varchar(50) not null
    ,country_name   varchar(50) not null
    ,is_capital     boolean     not null
)
;


alter table
    trip_cities
add constraint
    fk_city foreign key (city_id) references city (city_id)
;


alter table
    trip_cities
add constraint
    fk_trip foreign key (trip_id) references trip (trip_id)
;



insert into
    city (city_name, country_name, is_capital)
values
     ('Moscow',      'Russia',   True)
    ,('Nefteugansk', 'Russia',   False)
    ,('Dubai',       'UAE',      False)
    ,('Belgrade',    'Serbia',   True)
    ,('Bogota',      'Columbia', True)
;


insert into
    trip (trip_name, start_date, user_id)
values
     ('just_trip', date'2024-01-15', 123)
    ,('trip2',     date'2024-04-12', 321)
;


insert into
    trip_cities
select
     (select min(trip_id) from trip limit 1)  as trip_id
    ,coalesce(
        (select min(ord_number) from trip_cities limit 1)
        ,0
     ) + 1  as ord_number
    ,(select min(city_id) from city limit 1)  as trip_id
    ,10  as stay_duration
;


insert into
    trip_cities
select
     (select min(trip_id) + 1 from trip limit 1)  as trip_id
    ,coalesce(
        (select min(ord_number) from trip_cities limit 1)
        ,0
     ) + 1  as ord_number
    ,(select max(city_id) from city limit 1)  as trip_id
    ,5 as stay_duration
;
-- } task 1 end



-- { task 2 begin
drop table if exists guest         cascade;
drop table if exists building      cascade;
drop table if exists room          cascade;
drop table if exists booking       cascade;
drop table if exists building_copy cascade;

create table guest (
     guest_id  int           generated always as identity primary key
    ,name      text not null
    ,sex       varchar(10) -- who knows how many sexes we have now...
    ,age_years int  not null
)
;


create table building (
     code        text unique  -- we need the field to be unique - because it will be user as a foreign key in the "room" table
                              -- but it is not necessary the field to be a primary key
    ,room_cnt    int not null
    ,store_cnt   int not null
    ,description text
)
;


create table room (
     id            int         generated always as identity primary key
    ,square_m2     float not null
    ,bed_count     float not null -- sometimes it can be 1.5 beds
    ,window_view   json  -- to have an opportunity to expand parameters, and if we need to have a structure
    ,guest_count   int
    ,number        int not null
    ,store         int not null
    ,building_code text not null references building (code)
)
;


create table booking (
     id            int          generated always as identity primary key
    ,start_dt      timestamp
    ,duration_days float
    ,guest_id      int not null references guest (guest_id)
    ,room_id       int not null references room (id)
    ,meal_type     varchar(2)  --
)
;


insert into
    building
values
     ('1',  10,  3,  'oldest one')
    ,('3A', 100, 25, 'highest one')
;


insert into
    room (square_m2, bed_count, window_view, guest_count, number, store, building_code)
values
     (55.5, 2,   '{"garden": "almost"}',                    3, 1,  1,  '1')
    ,(33,   1.5, '{"garden": "yes", "ocean": "of course"}', 2, 2,  2,  '1')
    ,(40.1, 1,   '{"pool": "yes", "ocean": "a bit"}',       2, 2,  2,  '1')
    ,(55,   5,   '{"garden": "yes"}',                       5, 44, 10, '3A')
    ,(12,   1,   '{"ocean": "yes"}',                        1, 33, 15, '3A')
;


insert into
    guest (name, sex, age_years)
values
     ('petya',                'male',    33)
    ,('nurahamba petrosvani', 'unknown', 50)
;


insert into
    booking (start_dt, duration_days, guest_id, room_id, meal_type)
values
     ('2024-01-01 00:04:00', 3,  1, 1, 'HB')
    ,('2024-05-01 00:07:30', 3,  1, 2, 'HB')
    ,('2024-02-01 00:05:00', 15, 2, 2, 'BB')
;


insert into
    building
values
     ('1',  10,  3,  'oldest one was destroyed - this is the newest one')
    ,('3A', 100, 25, 'previous highest one')
    ,('5F', 200, 30, 'highest one')
on conflict (code) do
update
set
    description = excluded.description
;


delete from
    booking
where 1=1
    and id = (select min(id) from booking)
;


select
     *
    ,case
         when guest_id = min(guest_id) over (order by guest_id)
             then
                'this is the "select * from guest" query result'
        else
            null
    end  as just_a_comment
from
    guest
where 1=1
;


create table building_copy as
select * from building
;
-- } task 2 end