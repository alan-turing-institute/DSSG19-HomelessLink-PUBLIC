select 
-- false negatives
d.sum_self_FN,
d.sum_non_self_FN,


--sum after k
d.sum_self_after_k,
d.sum_non_self_after_k,

--false negatives / group size from semantic.alerts_with_features
d.sum_self_FN::float/(select sum(case when notifier_type = 'self' then 1 else 0 end) from 
(select a.score, b.notifier_type from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860) e ) as self_FN_GS,

d.sum_non_self_FN::float/(select sum(case when notifier_type != 'self' then 1 else 0 end)
from (
select a.score, b.notifier_type from results.predictions a
left join
semantic.alerts_with_features b on a.alert = b.alert
where model = 15860) e ) as non_self_FN_GS,


-- false negatives / predictived negatives
d.sum_self_FN::float/d.sum_self_after_k::float as self_FN_PN,
d.sum_non_self_FN::float / d.sum_non_self_after_k::float as non_self_FN_PN,


-- false negatives / total positive within fold (entire list - not broken by k)
d.sum_self_FN::float /(select sum(case when c.binary_self = 1 and label = 1 then 1 else 0 end) as test from
(select 
a.*,
case when notifier_type = 'self' then 1 else 0 end as binary_self
from (
select * from results.predictions
where model = 15860) a
left join
semantic.alerts_with_features b on a.alert = b.alert) c) as self_FN_LP,

d.sum_non_self_FN::float /(select sum(case when c.binary_self = 0 and label = 1 then 1 else 0 end) as test from
(select 
a.*,
case when notifier_type = 'self' then 1 else 0 end as binary_self
from (
select * from results.predictions
where model = 15860) a
left join
semantic.alerts_with_features b on a.alert = b.alert) c) as non_self_FN_LP


from(

select
sum(case when c.binary_self = 1 then 1 else 0 end) as sum_self_after_k, 
sum(case when c.binary_self = 0 then 1 else 0 end) as sum_non_self_after_k,

sum(case when c.binary_self = 1 and label = 1 then 1 else 0 end) as sum_self_FN,
sum(case when c.binary_self = 0 and label = 1 then 1 else 0 end) as sum_non_self_FN

from

(select 
a.*,
b.notifier_type,
case when b.notifier_type = 'self' then 1 else 0 end as binary_self
from (
select *, rank() over (order by score desc) from results.predictions
where model = 15860
order by score desc) a
left join
semantic.alerts_with_features b on a.alert = b.alert
where rank > 1000) c

) d 