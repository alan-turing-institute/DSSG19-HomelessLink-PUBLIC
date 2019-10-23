CREATE VIEW results.randomforest23_allUK_precision_recall_by_age_group AS (

with tbl as (
	select 
	p.as_of_date, 
	p.alert, 
	a.age_group,
	p.label, 
	p.score, 
	p.model, 
	e.experiment, e.fold, e.feature_set, e.param_config, e.algorithm,
	rank () over ( order by p.score desc) as k 
	from results.predictions p 
	join results.experiments e on e.model = p.model
	join semantic.alerts a on a.alert = p.alert
	where e.model = 15440
)
select 
	*,
	SUM(CASE WHEN label = 1 then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float / 
	NULLIF((SUM(CASE WHEN (label = 1 OR label = 0) then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row))::float,0) as precision_k,
	(SUM(CASE WHEN label = 1 then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float)/ 
	NULLIF((SUM(CASE WHEN (label = 1) then 1 else 0 end) 
		over (partition by model)::float),0) as recall_k, 
	COALESCE(
	SUM(CASE WHEN label = 1 and age_group = '25 - 50 years old' then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float / 
	NULLIF(SUM(CASE WHEN age_group = '25 - 50 years old' then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row),0)::float,0) 
		as precision_k_25_to_50,
	COALESCE(SUM(CASE WHEN label = 1 and age_group = '<25 years old' then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float / 
	NULLIF(SUM(CASE WHEN age_group = '<25 years old' then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row),0)::float,0) 
		as precision_k_under_25,

	COALESCE(
		SUM(CASE WHEN label = 1 and age_group = '> 50 years old' then 1 else 0 end) 
			OVER (order by k rows between unbounded preceding and current row)::float / 
		NULLIF(SUM(CASE WHEN age_group = '> 50 years old' then 1 else 0 end) 
			OVER (order by k rows between unbounded preceding and current row),0)::float,0) 
		as precision_k_over_50,
		
	COALESCE(
		SUM(CASE WHEN label = 1 and age_group = 'not known' then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float / 
	NULLIF(SUM(CASE WHEN age_group = 'not known' then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row),0)::float,0) 
		as precision_k_not_known,
	
	COALESCE(
		SUM(CASE WHEN label = 1 and age_group is null then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float / 
	NULLIF(SUM(CASE WHEN age_group is null then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row),0)::float,0) 
		as precision_k_no_age,
		
	(SUM(CASE WHEN label = 1 and age_group = '25 - 50 years old' then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float)/ 
	NULLIF((SUM(CASE WHEN (label = 1 and age_group = '25 - 50 years old') then 1 else 0 end) 
		over (partition by model)::float),0) as recall_k_25_to_50,

	(SUM(CASE WHEN label = 1 and age_group = '<25 years old' then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float)/ 
	NULLIF((SUM(CASE WHEN (label = 1 and age_group = '<25 years old') then 1 else 0 end) 
		over (partition by model)::float),0) as recall_k_under_25,

	(SUM(CASE WHEN label = 1 and age_group = '> 50 years old' then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float)/ 
	NULLIF((SUM(CASE WHEN (label = 1 and age_group = '> 50 years old') then 1 else 0 end) 
		over (partition by model)::float),0) as recall_k_over_50,

	(SUM(CASE WHEN label = 1 and age_group = 'not known' then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float)/ 
	NULLIF((SUM(CASE WHEN (label = 1 and age_group = 'not known') then 1 else 0 end) 
		over (partition by model)::float),0) as recall_k_not_known,

	(SUM(CASE WHEN label = 1 and age_group is null then 1 else 0 end) 
		OVER (order by k rows between unbounded preceding and current row)::float)/ 
	NULLIF((SUM(CASE WHEN (label = 1 and age_group is null) then 1 else 0 end) 
		over (partition by model)::float),0) as recall_k_no_age
	
from tbl
)