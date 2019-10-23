select 
-- false negatives
d.sum_london_FN,
d.sum_non_london_FN,


--sum after k
d.sum_london_after_k,
d.sum_non_london_after_k,

--false negatives / group size from semantic.alerts_with_features
d.sum_london_FN::float/(select sum(case when region = 'london' then 1 else 0 end) from 
(select a.score, b.region from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860) e ) as london_FN_GS,

d.sum_non_london_FN::float/(select sum(case when region != 'london' then 1 else 0 end)
from (
select a.score, b.region from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860) e ) as non_london_FN_GS,


-- false negatives / predictived negatives
d.sum_london_FN::float/d.sum_london_after_k::float as london_FN_PN,
d.sum_non_london_FN::float / d.sum_non_london_after_k::float as non_london_FN_PN,


-- false negatives / total positive within fold (entire list - not broken by k)
d.sum_london_FN::float /(select sum(case when c.binary_london = 1 and label = 1 then 1 else 0 end) as test from
(select 
a.*,
case when region = 'london' then 1 else 0 end as binary_london
from (
select * from results.predictions
where model = 15860) a
left join
semantic.alerts_with_features b on a.alert = b.alert) c) as london_FN_LP,

d.sum_non_london_FN::float /(select sum(case when c.binary_london = 0 and label = 1 then 1 else 0 end) as test from
(select 
a.*,
case when region = 'london' then 1 else 0 end as binary_london
from (
select * from results.predictions
where model = 15860) a
left join
semantic.alerts_with_features b on a.alert = b.alert) c) as non_london_FN_LP


from(

select
sum(case when c.binary_london = 1 then 1 else 0 end) as sum_london_after_k, 
sum(case when c.binary_london = 0 then 1 else 0 end) as sum_non_london_after_k,

sum(case when c.binary_london = 1 and label = 1 then 1 else 0 end) as sum_london_FN,
sum(case when c.binary_london = 0 and label = 1 then 1 else 0 end) as sum_non_london_FN

from

(select 
a.*,
b.region,
case when b.region = 'london' then 1 else 0 end as binary_london
from (
select *, rank() over (order by score desc) from results.predictions
where model = 15860
order by score desc) a
left join
semantic.alerts_with_features b on a.alert = b.alert
where rank > 1000) c

) d