-- top of model 
with top as (
	select 'top_1000' as name, p.model, p.label, p.score, e.algorithm, e.fold, e.feature_set, e.param_config, '1000'::float as total, 
	a.* 
	from results.predictions p
	join results.experiments e on e.model = p.model
	join semantic.alerts_with_features a on a.alert = p.alert
	where p.model = 15860
	order by p.score desc
	limit 1000
),
bottom as (
	select 'bottom_1000' as name, p.model, p.label, p.score, e.algorithm, e.fold, e.feature_set, e.param_config, '1000'::float as total, 
	a.*
	from results.predictions p
	join results.experiments e on e.model = p.model
	join semantic.alerts_with_features a on a.alert = p.alert
	where p.model = 15860
	order by p.score asc
	limit 1000
),
rest as (
	select 'bottom_11646' as name, p.model, p.label, p.score, e.algorithm, e.fold, e.feature_set, e.param_config, '11646'::float as total, 
	a.*
	from results.predictions p
	join results.experiments e on e.model = p.model
	join semantic.alerts_with_features a on a.alert = p.alert
	where p.model = 15860
	order by p.score asc
	limit 11646
),
top_metrics as (
select 
t.name,
t.algorithm, 
t.model, 
--person found rates
avg(t.person_found_rate_last_7d) as avg_person_found_rate_last_7d,
avg(t.person_found_rate_last_28d) as avg_person_found_rate_last_28d,
avg(t.referrals_within_1000m_7d) as avg_referrals_within_1000m_7d,
-- model score
avg(t.score) as avg_score, 
max(t.score) as max_score, 
min(t.score) as min_score, 
stddev(t.score) as stddev_score, 
-- weather
avg(t.avg_temperature) as avg_temp, 
avg(t.avg_windspeed) as avg_windspeed, 
-- distance from hotspot
avg(t.distance_to_nearest_hotspot) as avg_distance_from_hotspot,
stddev(t.distance_to_nearest_hotspot) as stddev_distance_from_hotspot,
min(t.distance_to_nearest_hotspot) as min_distance_from_hotspot,
max(t.distance_to_nearest_hotspot) as max_distance_from_hotspot,
-- appearance
avg(t.appearance_wordcount) as avg_appearance_wc, 
stddev(t.appearance_wordcount) as stddev_appearance_wc, 
min(t.appearance_wordcount) as min_appearance_wc, 
max(t.appearance_wordcount) as max_appearance_wc, 
-- location
avg(t.location_wordcount) as avg_location_wc,
stddev(t.location_wordcount) as stddev_location_wc,
min(t.location_wordcount) as min_location_wc,
max(t.location_wordcount) as max_location_wc,
-- concerns
avg(t.concerns_wordcount) as avg_concerns_wc,
stddev(t.concerns_wordcount) as stddev_concerns_wc,
min(t.concerns_wordcount) as min_concerns_wc,
max(t.concerns_wordcount) as max_concerns_wc,
-- gender
sum(t.gender_male)/t.total as total_male, 
sum(t.gender_female)/t.total as total_female,
sum(t.gender_nonbinary)/t.total as total_nonbinary,
-- time seen
sum(t.time_expected_flag)/t.total as time_expected_count,
sum(t.time_seen_flag)/t.total as time_seen_count,
-- alert origin
sum(t.alert_origin_web)/t.total as web_percent,
sum(t.alert_origin_phone)/t.total as phone_percent,
sum(t.alert_origin_mobile)/t.total as mobile_percent
from top t
group by t.algorithm, t.model, t.total, t.name),
bottom_metrics as (
select 
t.name,
t.algorithm, 
t.model, 
--person found rates
avg(t.person_found_rate_last_7d) as avg_person_found_rate_last_7d,
avg(t.person_found_rate_last_28d) as avg_person_found_rate_last_28d,
avg(t.referrals_within_1000m_7d) as avg_referrals_within_1000m_7d,
-- model score
avg(t.score) as avg_score, 
max(t.score) as max_score, 
min(t.score) as min_score, 
stddev(t.score) as stddev_score, 
-- weather
avg(t.avg_temperature) as avg_temp, 
avg(t.avg_windspeed) as avg_windspeed, 
-- distance from hotspot
avg(t.distance_to_nearest_hotspot) as avg_distance_from_hotspot,
stddev(t.distance_to_nearest_hotspot) as stddev_distance_from_hotspot,
min(t.distance_to_nearest_hotspot) as min_distance_from_hotspot,
max(t.distance_to_nearest_hotspot) as max_distance_from_hotspot,
-- appearance
avg(t.appearance_wordcount) as avg_appearance_wc, 
stddev(t.appearance_wordcount) as stddev_appearance_wc, 
min(t.appearance_wordcount) as min_appearance_wc, 
max(t.appearance_wordcount) as max_appearance_wc, 
-- location
avg(t.location_wordcount) as avg_location_wc,
stddev(t.location_wordcount) as stddev_location_wc,
min(t.location_wordcount) as min_location_wc,
max(t.location_wordcount) as max_location_wc,
-- concerns
avg(t.concerns_wordcount) as avg_concerns_wc,
stddev(t.concerns_wordcount) as stddev_concerns_wc,
min(t.concerns_wordcount) as min_concerns_wc,
max(t.concerns_wordcount) as max_concerns_wc,
-- gender
sum(t.gender_male)/t.total as total_male, 
sum(t.gender_female)/t.total as total_female,
sum(t.gender_nonbinary)/t.total as total_nonbinary,
-- time seen
sum(t.time_expected_flag)/t.total as time_expected_count,
sum(t.time_seen_flag)/t.total as time_seen_count,
-- alert origin
sum(t.alert_origin_web)/t.total as web_percent,
sum(t.alert_origin_phone)/t.total as phone_percent,
sum(t.alert_origin_mobile)/t.total as mobile_percent
from bottom t
group by t.algorithm, t.model, t.total, t.name
),

rest_metrics as (
select 
t.name,
t.algorithm, 
t.model, 
--person found rates
avg(t.person_found_rate_last_7d) as avg_person_found_rate_last_7d,
avg(t.person_found_rate_last_28d) as avg_person_found_rate_last_28d,
avg(t.referrals_within_1000m_7d) as avg_referrals_within_1000m_7d,
-- model score
avg(t.score) as avg_score, 
max(t.score) as max_score, 
min(t.score) as min_score, 
stddev(t.score) as stddev_score, 
-- weather
avg(t.avg_temperature) as avg_temp, 
avg(t.avg_windspeed) as avg_windspeed, 
-- distance from hotspot
avg(t.distance_to_nearest_hotspot) as avg_distance_from_hotspot,
stddev(t.distance_to_nearest_hotspot) as stddev_distance_from_hotspot,
min(t.distance_to_nearest_hotspot) as min_distance_from_hotspot,
max(t.distance_to_nearest_hotspot) as max_distance_from_hotspot,
-- appearance
avg(t.appearance_wordcount) as avg_appearance_wc, 
stddev(t.appearance_wordcount) as stddev_appearance_wc, 
min(t.appearance_wordcount) as min_appearance_wc, 
max(t.appearance_wordcount) as max_appearance_wc, 
-- location
avg(t.location_wordcount) as avg_location_wc,
stddev(t.location_wordcount) as stddev_location_wc,
min(t.location_wordcount) as min_location_wc,
max(t.location_wordcount) as max_location_wc,
-- concerns
avg(t.concerns_wordcount) as avg_concerns_wc,
stddev(t.concerns_wordcount) as stddev_concerns_wc,
min(t.concerns_wordcount) as min_concerns_wc,
max(t.concerns_wordcount) as max_concerns_wc,
-- gender
sum(t.gender_male)/t.total as total_male, 
sum(t.gender_female)/t.total as total_female,
sum(t.gender_nonbinary)/t.total as total_nonbinary,
-- time seen
sum(t.time_expected_flag)/t.total as time_expected_count,
sum(t.time_seen_flag)/t.total as time_seen_count,
-- alert origin
sum(t.alert_origin_web)/t.total as web_percent,
sum(t.alert_origin_phone)/t.total as phone_percent,
sum(t.alert_origin_mobile)/t.total as mobile_percent
from rest t
group by t.algorithm, t.model, t.total, t.name
)

select * from top_metrics 
union all 
select * from rest_metrics
union all
select * from bottom_metrics
