select 
-- false negatives
d.sum_female_FN,
d.sum_male_FN,
d.sum_non_binary_FN,
d.sum_unknown_FN,

--sum after k
d.sum_female_after_k,
d.sum_male_after_k,
d.sum_non_binary_after_k,
d.sum_unknown_after_k,

--false negatives / group size from semantic.alerts_with_features
d.sum_female_FN::float/(select count(e.gender)
from (
select a.score, b.gender from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860 and gender = 'female') e ) as female_FN_GS,

d.sum_male_FN::float/(select count(e.gender)
from (
select a.score, b.gender from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860 and gender = 'male') e ) as male_FN_GS,

d.sum_non_binary_FN::float/(select count(e.gender)
from (
select a.score, b.gender from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860 and gender = 'non-binary') e ) as non_binary_FN_GS,

d.sum_unknown_FN::float/(select count(e.gender)
from (
select a.score, b.gender from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860 and gender = 'unknown') e ) as unknown_FN_GS,



-- false negatives / predictived negatives
d.sum_female_FN::float/d.sum_female_after_k::float as female_FN_PN,
d.sum_male_FN::float / d.sum_male_after_k::float as male_FN_PN,
d.sum_non_binary_FN::float/d.sum_non_binary_after_k::float as non_binary_FN_PN,
d.sum_unknown_FN::float/d.sum_unknown_after_k::float as unknown_FN_PN,

-- false negatives / total positive within fold (entire list - not broken by k)
d.sum_female_FN::float /(select sum(case when c.gender = 'female' and label = 1 then 1 else 0 end) as test from
(select 
a.*,
b.gender
from (
select * from results.predictions
where model = 15860) a
left join
semantic.alerts_with_features b on a.alert = b.alert) c) as female_FN_LP,

d.sum_male_FN::float /(select sum(case when c.gender = 'male' and label = 1 then 1 else 0 end) as test from
(select 
a.*,
b.gender
from (
select * from results.predictions
where model = 15860) a
left join
semantic.alerts_with_features b on a.alert = b.alert) c) as male_FN_LP,

d.sum_non_binary_FN::float /(select sum(case when c.gender = 'non-binary' and label = 1 then 1 else 0 end) as test from
(select 
a.*,
b.gender
from (
select * from results.predictions
where model = 15860) a
left join
semantic.alerts_with_features b on a.alert = b.alert) c) as non_binary_FN_LP,

d.sum_unknown_FN::float /(select sum(case when c.gender = 'unknown' and label = 1 then 1 else 0 end) as test from
(select 
a.*,
b.gender
from (
select * from results.predictions
where model = 15860) a
left join
semantic.alerts_with_features b on a.alert = b.alert) c) as unknown_FN_LP


from(

select
sum(case when c.gender = 'female' then 1 else 0 end) as sum_female_after_k, 
sum(case when c.gender = 'male' then 1 else 0 end) as sum_male_after_k,
sum(case when c.gender = 'non-binary' then 1 else 0 end) as sum_non_binary_after_k,
sum(case when c.gender = 'unknown' then 1 else 0 end) as sum_unknown_after_k,

sum(case when c.gender = 'female' and label = 1 then 1 else 0 end) as sum_female_FN,
sum(case when c.gender = 'male' and label = 1 then 1 else 0 end) as sum_male_FN,
sum(case when c.gender = 'non-binary' and label = 1 then 1 else 0 end) as sum_non_binary_FN,
sum(case when c.gender = 'unknown' and label = 1 then 1 else 0 end) as sum_unknown_FN

from

(select 
a.*,
b.gender
from (
select *, rank() over (order by score desc) from results.predictions
where model = 15860
order by score desc) a
left join
semantic.alerts_with_features b on a.alert = b.alert
where rank > 1000) c

) d