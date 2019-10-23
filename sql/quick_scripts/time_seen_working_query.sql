with time_seen_modified as (
select 
time_seen,
regexp_matches(
CASE WHEN time_seen ilike '%p%' THEN (substring(time_seen,'[0-9]+')::int + 12)::varchar
ELSE time_seen END ,'[0-9]{1,2}')
as new_time_seen
from raw.alerts_raw
)
select  
time_seen, new_time_seen[1], count(*)
from time_seen_modified
group by time_seen, new_time_seen[1]
order by count(*) desc






