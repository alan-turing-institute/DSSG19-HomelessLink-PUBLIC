SELECT 
cosd(360 * extract(dow from datetime_opened) / 7) as dowx, 
sind(360 * extract(dow from datetime_opened) / 7) as dowy, 
extract(dow from datetime_opened),
cosd(360 * extract(day from datetime_opened) / date_part('days', date_trunc('month', datetime_opened) + interval '1 month' - interval '1 day')) as domx,
sind(360 * extract(day from datetime_opened) / date_part('days', date_trunc('month', datetime_opened) + interval '1 month' - interval '1 day')) as domy,
extract(day from datetime_opened),
-- Fix for leap years ?
cosd(360 * extract(doy from datetime_opened) / 365) as doyx,
sind(360 * extract(doy from datetime_opened) / 365) as doyy,
extract(doy from datetime_opened),
extract(day from date_trunc('day', datetime_opened - '2010-01-01')),
datetime_opened
FROM semantic.alerts limit 1000;
