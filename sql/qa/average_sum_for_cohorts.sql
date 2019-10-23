select
avg(b.count_of_alerts)::int as alert_count,
avg(person_not_found_count)::int as person_not_found_count,
avg(b.count_of_person_found)::int as person_found_count
 from (
select
--date_trunc('month', a.datetime_opened),
COUNT(distinct a.alert) as count_of_alerts,
SUM(CASE WHEN a.outcome_internal in ('person not found - site could not be located', 'person not found - site not accessible', 'person not found - person looked for', 'person not looked for - street activity/begging location') THEN 1 ELSE 0 END) as person_not_found_count,
SUM(CASE WHEN a.outcome_internal in ('person found - engaging with services', 'person found - no entitlement to local services', 'person found - reconnected to services in another area', 'person found - street activity/begging', 'person found - referred to emergency services', 'person found - taken to shelter', 'person found - unwilling to engage', 'person not found - person no longer rough sleeping', 'person not looked for - person already known') and a.date_closed < a.datetime_opened + interval '1w' THEN 1 ELSE 0 END) as count_of_person_found
from semantic.alerts_with_features a
WHERE a.datetime_opened::date >= '2017-12-21'
AND a.datetime_opened::date <= '2018-01-21'
--AND a.region = 'london'
--GROUP BY date_trunc('month', a.datetime_opened)
) b;

--select * from semantic.alerts_with_features_no_outliers limit 10
--select * alert from semantic.alerts

-- select distinct a.alert, a.score, b.datetime_opened, b.date_closed, b.outcome_internal, a.label, case when a.label = 1 then 1 else 0 end from results.predictions a
-- inner join semantic.alerts_with_features b on a.alert = b.alert
-- where model in (
--     select model from results.experiments
--     where experiment = 2 and fold = 2 and param_config = 14 order by model desc limit 1
-- ) and label = 1 order by score desc
