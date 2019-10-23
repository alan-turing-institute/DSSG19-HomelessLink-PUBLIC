with tbl as (
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
	p.value as p_list,
	r.value as r_list,
	cast(p.value as jsonb) as precision,
	r.key::date as recall_as_of_date,
	cast(r.value as jsonb) as recall
	from results.experiments e
	join json_each_text(precision_at_k::json) p on TRUE 
	join json_each_text(recall_at_k::json) r on TRUE 
	where algorithm != 'Baseline'
	and p.key::date = r.key::date
)
select 
t.model, t.experiment, t.fold, t.feature_set, t.param_config, t.algorithm, count(a.elem) as count_of_recall_above_1
--t.precision_as_of_date, avg(a.elem::float) as precision_k_avg, a.elem as precision_k, a.nr as k_p, b.elem as recall_k, b.nr as k_r,
FROM tbl as t 
left join lateral unnest(string_to_array(substr(t.r_list,2,length(t.r_list)-2),', ')) WITH ORDINALITY AS a(elem, nr) on TRUE
where a.elem::float > 1
group by t.model, t.experiment, t.fold, t.feature_set, t.param_config, t.algorithm
ORDER BY 1 DESC