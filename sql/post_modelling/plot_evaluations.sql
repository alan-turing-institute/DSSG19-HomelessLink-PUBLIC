select 
e.model,
e.experiment,
e.fold,
e.feature_set,
e.param_config,
e.algorithm,
e.date_ran,
NULL::varchar as k,
p.key::date as precision_as_of_date,
cast(p.value as jsonb) as precision,
r.key::date as recall_as_of_date,
cast(r.value as jsonb) as recall
from results.experiments e
join json_each_text(precision_at_k::json) p on TRUE 
join json_each_text(recall_at_k::json) r on TRUE 
where algorithm != 'Baseline' and p.key::date = r.key::date

union all

select
e.model,
e.experiment,
e.fold,
e.feature_set,
e.param_config,
e.algorithm,
e.date_ran,
split_part(split_part(p.value::varchar,':',1),'"',2)::varchar as k,
p.key::date as precision_as_of_date, 
cast(split_part(split_part(p.value::varchar,': ',2),'}',1) as jsonb) as precision,
NULL::date as recall_as_of_date,
cast('-1' as jsonb) as recall
from results.experiments e
join json_each_text(precision_at_k::json) p on TRUE 
where algorithm = 'Baseline'

