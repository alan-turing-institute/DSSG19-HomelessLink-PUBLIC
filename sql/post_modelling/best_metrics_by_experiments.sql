with params_configs as (

select
	experiment, algorithm, param_config, model from results.experiments where experiment in (62,77,74)
),

cf as (
select param_config, algorithm,model, sum(label) over (partition by model order by score desc) as tp,
sum(case when label in (0,1) then 1 else 0 end) over (partition by model order by score desc) as labelled,
row_number() over (partition by model order by score desc) as k,
 sum(label) over(partition by model) as pos ,
 sum(case when label is null then 1 else 0 end) over (partition by model order by score desc) as number_of_nulls
from params_configs left join results.predictions  using(model)
),


metrics as (
select param_config, k, algorithm, model,
tp::float/labelled as precision, tp::float/pos as recall , number_of_nulls
from cf
where k = 1649
 )

-- select * from metrics where algorithm ilike '%dec%'


select
 param_config, k,
 algorithm, avg(precision) as "AVG(PRECISION)", avg(recall) as "AVG(RECALL)",
 stddev(precision) as "STD(PRECISION)", stddev(recall) as "STD(RECALL)",
 min(precision) as "MIN(PRECISION)", min(recall) as "MIN(RECALL)",
 max(precision) as "MAX(PRECISION)", max(recall) as "MAX(RECALL)",
 avg(number_of_nulls)
from metrics
group by param_config, k, algorithm
order by 5  desc


-- select *, sum(case when label in (0,1) then 1 else 0 end) over (partition by model order by score desc) as k from results.predictions where model = 11701 order by score desc
