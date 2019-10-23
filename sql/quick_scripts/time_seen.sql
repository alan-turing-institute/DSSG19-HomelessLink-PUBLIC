with tbl as (
select 
extract(year from a.datetime_opened) as year,
extract(month from a.datetime_opened) as month, 
sum(case when a.time_seen is not null OR a.time_expected is not null then 1 else 0 end) as time_seen_or_expected,
sum(case when a.time_seen is null AND a.time_expected is null then 1 else 0 end) as time_not_seen
from semantic.alerts a
where 
a.datetime_opened >= '2017-01-01' 
and a.datetime_opened <= '2019-02-28'
and a.alert_origin in ('phone','email')
group by
extract(year from a.datetime_opened),
extract(month from a.datetime_opened)
)
select t.*, (t.time_not_seen::float/(t.time_not_seen::float + t.time_seen_or_expected::float))::decimal(4,2) as time_not_seen_percentage 
from  tbl t
order by t.year asc, t.month, t.time_not_seen desc