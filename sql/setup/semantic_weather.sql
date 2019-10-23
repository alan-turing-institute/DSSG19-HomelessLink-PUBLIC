-- ======
-- Description: 
-- 	1. Drops if exists semantic.weather
-- 	2. Creates semantic.weather table
-- Last Updated: Aug 29, 2019
-- ======

drop table if exists semantic.weather;
create table if not exists semantic.weather as (
	select
	t1.datetime_opened as gen_date_weather,
	t2.*
	from (
		(select 
		distinct datetime_opened::date 
		from semantic.alerts 
		order by datetime_opened asc) t1
	left join (
		select 
			CAST(date_time_weather as DATE) as date_weather,
			round(AVG(temperature)::numeric,2) as avg_temperature,
			MIN(temperature) as min_temperature,
			MAX(temperature) as max_temperature,
			round(AVG(windspeed)::numeric,2) as avg_windspeed,
			MAX(windgust)  as max_windgust,
			MAX(precipprobability) as max_precipprobability,
			MIN(precipprobability) as min_precipprobability,
			CASE WHEN SUM(precipaccumulation) > 0 then 1 else 0 end as snowaccumulation_flag
		from cleaned.weather
		GROUP BY CAST(date_time_weather as DATE) ) t2 on t1.datetime_opened::date = t2.date_weather::date 
	)
);