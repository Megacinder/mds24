if not exists (select 1 from sys.databases where name = 'dev')
    create database dev


if not exists (select 1 from sys.databases where name = 'prod')
    create database prod



if not exists(select 1 from sys.servers where name = 'linked_serv')
    exec sp_addlinkedserver
        @server     = 'linked_serv'
       ,@provider   = 'SQLNCLI'
       ,@datasrc    = 'localhost'
       ,@srvproduct = ''
       ,@provstr    = 'Integrated Security=SSPI'
    -- exec sp_dropserver "linked_serv"
;

use dev

if not exists (select 1 from sys.objects where lower(name) = 'lk_prod_to_dev' and type = 'u')
    create table dev.dbo.lk_prod_to_dev (
         table_name nvarchar(300)
        ,action nvarchar(100)
    )

insert into dev.dbo.lk_prod_to_dev
values
 ('%_SCD', 'Copy with data')
,('%_History', 'Skip')
,('ya_table', 'Skip')
,('ya_table1', 'Skip')

select 11  as id into history_scd
select 21  as id into scd_history
select 31  as id into not_just_history_but


use prod
select 1  as id into history_scd
select 2  as id into scd_history
select 3  as id into just_history
select 4  as id into ya_table1
select 5  as id into ya_table2
select 6  as id into table_scd
select 7  as id into dim_scd
select 8  as id into dim_history
select 9  as id into z_dim_history
select 10  as id into z_dim_scd
select 11  as id into z_history_scd
