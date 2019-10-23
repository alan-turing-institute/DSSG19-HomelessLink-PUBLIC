select 
d.sum_not_known_FN,
d.sum_under_25_FN,
d.sum_25_50_FN,
d.sum_over_50_FN,
d.sum_not_known_FN::float/d.sum_not_known_after_k::float as not_known_FN_PN,
d.sum_under_25_FN::float / d.sum_under_25_after_k::float as under_25_FN_PN,
d.sum_25_50_FN::float/d.sum_25_50_after_k::float as s25_50_FN_PN,
d.sum_over_50_FN::float/d.sum_over_50_after_k::float as over_50_FN_PN,
d.sum_under_25_FN::float /(select sum(case when c.age_group = '<25 years old' then 1 else 0 end) as test from
(select 
a.*,
b.age_group
from (
select *, rank() over (order by score desc) from results.predictions
where model = 15860
order by score desc) a
left join
semantic.alerts_with_features b on a.alert = b.alert
where rank <= 1000) c) as under_25_FN_LP,

d.sum_25_50_FN::float /(select sum(case when c.age_group = '25 - 50 years old' then 1 else 0 end) as test from
(select 
a.*,
b.age_group
from (
select *, rank() over (order by score desc) from results.predictions
where model = 15860
order by score desc) a
left join
semantic.alerts_with_features b on a.alert = b.alert
where rank <= 1000) c) as s25_50_FN_LP,

d.sum_over_50_FN::float /(select sum(case when c.age_group = '> 50 years old' then 1 else 0 end) as test from
(select 
a.*,
b.age_group
from (
select *, rank() over (order by score desc) from results.predictions
where model = 15860
order by score desc) a
left join
semantic.alerts_with_features b on a.alert = b.alert
where rank <= 1000) c) as over_50_FN_LP,

d.sum_not_known_FN::float /(select sum(case when c.age_group = 'not known' then 1 else 0 end) as test from
(select 
a.*,
b.age_group
from (
select *, rank() over (order by score desc) from results.predictions
where model = 15860
order by score desc) a
left join
semantic.alerts_with_features b on a.alert = b.alert
where rank <= 1000) c) as not_known_FN_LP,

d.sum_25_50_FN::float/(select count(e.age_group)
from (
select a.score, b.age_group from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860 and age_group = '25 - 50 years old') e ) as s25_50_FN_GS,

d.sum_under_25_FN::float/(select count(e.age_group)
from (
select a.score, b.age_group from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860 and age_group = '<25 years old') e ) as under_25_FN_GS,

d.sum_over_50_FN::float/(select count(e.age_group)
from (
select a.score, b.age_group from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860 and age_group = '> 50 years old') e ) as over_50_FN_GS,

d.sum_not_known_FN::float/(select count(e.age_group)
from (
select a.score, b.age_group from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860 and age_group = 'not known') e ) as not_known_FN_GS

from(

select
sum(case when c.age_group = 'not known' then 1 else 0 end) as sum_not_known_after_k, 
sum(case when c.age_group = '<25 years old' then 1 else 0 end) as sum_under_25_after_k,
sum(case when c.age_group = '25 - 50 years old' then 1 else 0 end) as sum_25_50_after_k,
sum(case when c.age_group = '> 50 years old' then 1 else 0 end) as sum_over_50_after_k,
sum(case when c.age_group = 'not known' then 1 else 0 end) as sum_not_known_FN,
sum(case when c.age_group = '<25 years old' and label = 1 then 1 else 0 end) as sum_under_25_FN,
sum(case when c.age_group = '25 - 50 years old' and label = 1 then 1 else 0 end) as sum_25_50_FN,
sum(case when c.age_group = '> 50 years old' and label = 1 then 1 else 0 end) as sum_over_50_FN

from

(select 
a.*,
b.age_group
from (
select *, rank() over (order by score desc) from results.predictions
where model = 15860
order by score desc) a
left join
semantic.alerts_with_features b on a.alert = b.alert
where rank > 1000) c

) d




--need it to be fold 13