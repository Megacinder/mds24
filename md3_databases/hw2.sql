-- 1.

select distinct
     a.region
    ,a.continent
    ,current_timestamp
from
    country  a
where 1=1
    and a.indepyear between 1900 and 1920
    and a.lifeexpectancy > 70
    and case when a.population = 0 then 0 else a.surfacearea / a.population end > 0.05


-- 2.

select
     a.name  as country_name
    ,a.localname  as country_local_name
    ,current_timestamp
from
    country  a
where 1=1
    and a.name != a.localname
order by
    a.name


-- 3.

select
     a.continent
    ,(array_agg(a.name       order by a.population desc, a.name))[1]  as most_populated_country
    ,(array_agg(a.population order by a.population desc, a.name))[1]  as population_of_the_most_populated_country
    ,current_timestamp
from
    country  a
where 1=1
group by
    a.continent


-- 4.

select distinct
    a.governmentform
    ,current_timestamp
from
    country  a
where 1=1
    and a.lifeexpectancy < 61
    and a.continent in ('Asia', 'Europe')
order by
    a.governmentform


-- 5.

select
    a.name
    ,current_timestamp
from
    country  a
where 1=1
    and a.name ilike '%' || a.code || '%'
order by
    a.name


-- 6.

select
     a.flight_id
--    ,a.actual_arrival
--    ,a.actual_departure
    ,(extract(epoch from a.actual_arrival) - extract(epoch from a.actual_departure)) / 60  as flight_dur_mi
    ,current_timestamp
from
    flights  a
where 1=1
    and a.arrival_airport = 'DME'
    and a.actual_departure  >= '2017-02-20 15:00:00' :: timestamp
    and a.actual_departure  <  '2017-02-20 18:00:00' :: timestamp
order by
    a.flight_id


-- 7.

select
     a.countrycode
    ,b.name  as country_name
    ,a.language
    ,a.isofficial  as is_the_lang_official
    ,b.population * a.percentage / 100  as people_cnt_speaking_the_language
    ,current_timestamp
from
    countrylanguage  a
    join country  b
        on b.code = a.countrycode
where 1=1
    and a.percentage >= 30
order by
     b.name
    ,case when a.isofficial then 1 else 2 end
    ,b.population * a.percentage / 100 desc



-- 8.

with wt_english_city as (
    select
         a.countrycode
        ,c.name  as city_name
        ,a.language
        ,c.population
        -- it can be several cities with the same population of language-speaking people
        --  so the "limit" is not suitable for us
        ,dense_rank() over (order by c.population desc)  as rank1
    from
        countrylanguage  a
        join city  c
            on c.countrycode = a.countrycode
    where 1=1
        and a."language" = 'English'
)

select
     city_name
    ,population  as english_speaking_people
    ,current_timestamp
from
    wt_english_city
where 1=1
    and rank1 <= 10
order by
    rank1


-- 9

with wt_pivot as (
    select distinct 'Континент'  as geo_type,  a.continent  as geo_name from country  a union all
    select          'Страна'     as geo_type,  a.name       as geo_name from country  a union all
    select distinct 'Регион'     as geo_type,  a.region     as geo_name from country  a
)

select
     left(geo_name, 1)  as first_letter
    ,geo_name
    ,geo_type
    ,current_timestamp
from
    wt_pivot
where 1=1
order by
    geo_name


-- 10.

with wt_prep as (
    select
         a.continent
        ,a.name  as country
        ,b.name  as city_name
        ,b.population  as city_population

        ,first_value(b.population) over (partition by a.name order by case when a.capital = b.id then 0 else 1 end)  as capital_population
        ,first_value(b.name)       over (partition by a.name order by case when a.capital = b.id then 0 else 1 end)  as capital_name
    from
        country  a
        join city  b
            on b.countrycode = a.code
    where 1=1
)

select
     case
        when capital_population = 0
            then 0
        else round(city_population :: numeric / capital_population * 100, 2)
     end  as population_precentage_rate_from_capital

    ,city_name
    ,capital_name
    ,country
    ,continent
    ,current_timestamp
from
    wt_prep
where 1=1
--    and country = 'Côte d’Ivoire'
order by
     continent
    ,city_name
