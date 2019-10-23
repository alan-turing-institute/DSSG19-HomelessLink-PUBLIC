with outcomes as (select distinct
outcome_summary,
outcome_internal,
count(*) as total
from
raw.alerts_processed 
group by 
outcome_summary,
outcome_internal ) 
select 
outcome_internal,
total,
(100*total)/sum(total) over () as percentage
from outcomes o
group by 
outcome_internal,
total
order by percentage desc

