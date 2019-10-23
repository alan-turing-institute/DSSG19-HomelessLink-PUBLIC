CREATE VIEW results.randomForest23_allUK_precision_recall_by_gender AS (

with tbl as (
	select
	p.as_of_date,
	p.alert,
	a.gender,
	p.label,
	p.score,
	p.model,
	e.experiment,
	e.fold, 
	e.feature_set, 
	e.param_config, 
	e.algorithm,
	rank () over ( order by p.score desc) as k
	from results.predictions p
	join results.experiments e on e.model = p.model
	join semantic.alerts a on a.alert = p.alert
	where e.model = 15440
)
select
	*,
	--SUM(CASE WHEN label = 1 then 1 else 0 end) OVER (order by k rows between unbounded preceding and current row) as cum_sum_only_positives,
	--SUM(CASE WHEN (label = 1 OR label = 0) then 1 else 0 end) OVER (order by k rows between unbounded preceding and current row) as tp_fp,
	SUM(CASE WHEN label = 1 then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float /
	(SUM(CASE WHEN (label = 1 OR label = 0) then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row))::float as precision_k,
	(SUM(CASE WHEN label = 1 then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float)/
	(SUM(CASE WHEN (label = 1) then 1 else 0 end)
		over (partition by model)::float) as recall_k,
	COALESCE(
	SUM(CASE WHEN label = 1 and gender = 'male' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float /
	NULLIF(SUM(CASE WHEN gender = 'male' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row),0)::float,0)
		as precision_k_male,
	COALESCE(SUM(CASE WHEN label = 1 and gender = 'female' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float /
	NULLIF(SUM(CASE WHEN gender = 'female' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row),0)::float,0)
		as precision_k_female,
	COALESCE(
		SUM(CASE WHEN label = 1 and gender = 'unknown' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float /
	NULLIF(SUM(CASE WHEN gender = 'unknown' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row),0)::float,0)
		as precision_k_unknown,
	COALESCE(
		SUM(CASE WHEN label = 1 and gender = 'non-binary' then 1 else 0 end)
			OVER (order by k rows between unbounded preceding and current row)::float /
		NULLIF(SUM(CASE WHEN gender = 'non-binary' then 1 else 0 end)
			OVER (order by k rows between unbounded preceding and current row),0)::float,0)
		as precision_k_nonbinary,
	COALESCE(
		SUM(CASE WHEN label = 1 and gender is null then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float /
	NULLIF(SUM(CASE WHEN gender is null then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row),0)::float,0)
		as precision_k_no_gender,

	(SUM(CASE WHEN label = 1 and gender = 'male' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float)/
	NULLIF((SUM(CASE WHEN (label = 1 and gender = 'male') then 1 else 0 end)
		over (partition by model)::float),0) as recall_k_male,

	(SUM(CASE WHEN label = 1 and gender = 'female' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float)/
	NULLIF((SUM(CASE WHEN (label = 1 and gender = 'female') then 1 else 0 end)
		over (partition by model)::float),0) as recall_k_female,

	(SUM(CASE WHEN label = 1 and gender = 'unknown' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float)/
	NULLIF((SUM(CASE WHEN (label = 1 and gender = 'unknown') then 1 else 0 end)
		over (partition by model)::float),0) as recall_k_unknown,

	(SUM(CASE WHEN label = 1 and gender = 'non-binary' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float)/
	NULLIF((SUM(CASE WHEN (label = 1 and gender = 'non-binary') then 1 else 0 end)
		over (partition by model)::float),0) as recall_k_nonbinary,

	(SUM(CASE WHEN label = 1 and gender is null then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)::float)/
	NULLIF((SUM(CASE WHEN (label = 1 and gender is null) then 1 else 0 end)
		over (partition by model)::float),0) as recall_k_no_gender




	/*,SUM(CASE WHEN label = 1 and gender = 'male' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)
		as true_label_gender_male,
	SUM(CASE WHEN gender = 'male' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)
		as total_gender_male,
	SUM(CASE WHEN label = 1 and gender = 'unknown' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)
		as true_label_gender_unknown,
	SUM(CASE WHEN gender = 'unknown' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)
		as total_gender_unknown,
	SUM(CASE WHEN gender = 'female' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)
		as total_gender_female,
	SUM(CASE WHEN label = 1 and gender = 'female' then 1 else 0 end)
		OVER (order by k rows between unbounded preceding and current row)
		as true_label_gender_female*/
from tbl
)