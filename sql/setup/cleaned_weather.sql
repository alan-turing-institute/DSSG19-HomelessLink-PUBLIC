-- ======
-- Description: 
-- 	1. Drops if exists cleaned.weather
-- 	2. Creates table cleaned.weather data from raw.weathers_raw
-- Last Updated: Aug 29, 2019
-- ======

DROP TABLE IF EXISTS cleaned.weather;

CREATE TABLE if not exists cleaned.weather AS (
SELECT
to_timestamp(NULLIF(time,''), 'YYYY/MM/DD HH24:MI:SS') as date_time_weather,
NULLIF(summary,'')::varchar as summary,
NULLIF(icon,'')::varchar as icon,
NULLIF(precipIntensity,'NA')::float as precipIntensity,
NULLIF(precipProbability,'NA')::float as precipProbability,
NULLIF(temperature,'NA')::float as temperature,
NULLIF(apparentTemperature,'NA')::float as pparentTemperature,
NULLIF(dewPoint,'NA')::float as dewPoint,
NULLIF(humidity,'NA')::float as humidity,
NULLIF(pressure,'NA')::float as pressure,
NULLIF(windSpeed,'NA')::float as windSpeed,
NULLIF(windGust,'NA')::float as windGust,
NULLIF(windBearing,'NA')::float as windBearing,
NULLIF(cloudCover,'NA')::float as cloudCover,
NULLIF(uvIndex,'NA')::float as uvIndex,
NULLIF(precipAccumulation,'NA')::float as precipAccumulation
from raw.weather_raw
)
