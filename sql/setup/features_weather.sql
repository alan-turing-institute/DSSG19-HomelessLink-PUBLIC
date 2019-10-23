-- ======
-- Description: 
-- 	1. Drops if exists features.weather
-- 	2. Creates various features from cleaned.weather data, filling in any null values with last non-null value
--	3. Creates index on the gen_date_weather column so that joins with other tables are fast
-- Last Updated: Aug 29, 2019
-- ======

DROP TABLE IF EXISTS features.weather;
CREATE TABLE IF NOT EXISTS features.weather as (
SELECT
	gen_date_weather,
	date_weather,
	first_value(avg_temperature) over (partition by grp_avg_temperature) as avg_temperature,
	first_value(min_temperature) over (partition by grp_min_temperature) as min_temperature,
	first_value(max_temperature) over (partition by grp_max_temperature) as max_temperature,
	first_value(avg_windspeed) over (partition by grp_avg_windspeed) as avg_windspeed,
	first_value(max_windgust) over (partition by grp_max_windgust) as max_windgust,
	first_value(max_precipprobability) over (partition by grp_max_precipprobability) as max_precipprobability,
	first_value(min_precipprobability) over (partition by grp_min_precipprobability) as min_precipprobability,
	case when snowaccumulation_flag > 0 then 1 else 0 end as snowaccumulation_flag
FROM
(
	SELECT
		CAST(gen_date_weather as DATE) as gen_date_weather,
		date_weather,
		max_windgust,
		max_precipprobability,
		min_precipprobability,
		avg_temperature,
		min_temperature,
		max_temperature,
		avg_windspeed,
		snowaccumulation_flag,
		sum(case when avg_temperature is not null then 1 end) over (order by CAST(gen_date_weather as DATE)) as grp_avg_temperature,
		sum(case when min_temperature is not null then 1 end) over (order by CAST(gen_date_weather as DATE)) as grp_min_temperature,
		sum(case when max_temperature is not null then 1 end) over (order by CAST(gen_date_weather as DATE)) as grp_max_temperature,
		sum(case when avg_windspeed is not null then 1 end) over (order by CAST(gen_date_weather as DATE))  as grp_avg_windspeed,
		sum(case when max_windgust is not null then 1 end) over (order by CAST(gen_date_weather as DATE))  as grp_max_windgust,
		sum(case when max_precipprobability is not null then 1 end) over (order by CAST(gen_date_weather as DATE))  as grp_max_precipprobability,
		sum(case when min_precipprobability is not null then 1 end) over (order by CAST(gen_date_weather as DATE))  as grp_min_precipprobability
		from semantic.weather) t
);

CREATE INDEX weather_gen_date_weather_ix on features.weather (gen_date_weather);
