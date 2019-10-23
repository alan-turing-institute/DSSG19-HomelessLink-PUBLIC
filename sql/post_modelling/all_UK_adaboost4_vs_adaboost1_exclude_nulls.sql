with ada as (
select 
'key' as key_to_match,
e.model, e.experiment, e.fold, e.param_config, p.alert, p.label, p.score,
row_number() over (partition by e.model order by p.score desc) as row_n
from results.predictions p 
join results.experiments e on p.model = e.model
where e.fold = 13 and label is not null and
(e.algorithm ilike '%ada%' and e.param_config = 4 and e.experiment = 74) 
--(e.algorithm ilike '%random%' and e.param_config = 24 and e.experiment = 61) 
limit 1000
),
rf as (
select 
'key' as key_to_match,
e.model, e.experiment, e.fold, e.param_config, p.alert, p.label, p.score,
row_number() over (partition by e.model order by p.score desc) as row_n
from results.predictions p 
join results.experiments e on p.model = e.model
where e.fold = 13 and p.label is not null and 
e.algorithm ilike '%ada%' and e.param_config = 1 and e.experiment = 62
--(e.algorithm ilike '%random%' and e.param_config = 23 and e.experiment = 62) 
--(e.algorithm ilike '%random%' and e.param_config = 20 and e.experiment = 61) 
limit 1000
),
u as (
select 'key' as key_to_match, 
count(distinct a.alert) as union_of_list from (
(
select 
e.model, e.experiment, e.fold, e.param_config, p.alert, p.label, p.score,
row_number() over (partition by e.model order by p.score desc) as row_n
from results.predictions p 
join results.experiments e on p.model = e.model
where e.fold = 13 and p.label is not null and 
(e.algorithm ilike '%ada%' and e.param_config = 4 and e.experiment = 74)
--(e.algorithm ilike '%random%' and e.param_config = 20 and e.experiment = 61)  
limit 1000
)

UNION 

(select 
e.model, e.experiment, e.fold, e.param_config, p.alert, p.label, p.score,
row_number() over (partition by e.model order by p.score desc) as row_n
from results.predictions p 
join results.experiments e on p.model = e.model
where e.fold = 13 and p.label is not null and 
e.algorithm ilike '%ada%' and e.param_config = 1 and e.experiment = 62
--(e.algorithm ilike '%random%' and e.param_config = 23 and e.experiment = 62)
--(e.algorithm ilike '%random%' and e.param_config = 24 and e.experiment = 61)  
limit 1000) )  as a

)
select 
u.union_of_list,
count(a.alert) as intersection,
count(a.alert)::float / u.union_of_list::float as jaccard_similarity
from ada a
inner join rf r on r.alert = a.alert
left join u on u.key_to_match = a.key_to_match
group by u.union_of_list