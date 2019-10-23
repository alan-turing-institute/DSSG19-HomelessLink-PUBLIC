-- ======
-- Description: 
-- 	1. Drops if exists semantic.alerts_with_features_no_outliers
-- 	2. Creates table removing days with more than 1000 alerts created within a single day for better training purposes
-- Status: Not part of main pipeline. Could be useful for future iterations. 
-- Last Updated: Aug 29, 2019
-- ======

DROP TABLE IF EXISTS semantic.alerts_with_features_no_outliers;
CREATE TABLE IF NOT EXISTS semantic.alerts_with_features_no_outliers as (
select 
	t1.*,
	t2.gen_date_weather,
	t2.avg_temperature,
	t2.min_temperature,
	t2.max_temperature,
	t2.avg_windspeed,
	t2.max_windgust,
	t2.max_precipprobability,
	t2.min_precipprobability,
	t2.snowaccumulation_flag,
 	t3.la_avg_response_time_7d,
	t3.la_avg_response_time_28d,
	t3.alerts_last_7d,
	t3.alerts_last_28d,
	t3.person_found_last_7d,
	t3.person_found_last_28d,
	t3.referrals_last_7d,
	t3.referrals_last_28d,
	t3.person_found_rate_last_7d,
	t3.person_found_rate_last_28d,
	t4.alerts_within_50m_7d,
	t4.alerts_within_50m_28d,
	t4.alerts_within_50m_60d,
	t4.alerts_within_50m_6mo,
	t4.alerts_within_250m_7d,
	t4.alerts_within_250m_28d,
	t4.alerts_within_250m_60d,
	t4.alerts_within_250m_6mo,
	t4.alerts_within_1km_7d,
	t4.alerts_within_1km_28d,
	t4.alerts_within_1km_60d,
	t4.alerts_within_1km_6mo,
	t4.referrals_within_50m_7d,
	t4.referrals_within_50m_28d,
	t4.referrals_within_50m_60d,
	t4.referrals_within_50m_6mo,
	t4.referrals_within_250m_7d,
	t4.referrals_within_250m_28d,
	t4.referrals_within_250m_60d,
	t4.referrals_within_250m_6mo,
	t4.referrals_within_1km_7d,
	t4.referrals_within_1km_28d,
	t4.referrals_within_1km_60d,
	t4.referrals_within_1km_6mo,
	t4.persons_found_within_50m_7d,
	t4.persons_found_within_50m_28d,
	t4.persons_found_within_50m_60d,
	t4.persons_found_within_50m_6mo,
	t4.persons_found_within_250m_7d,
	t4.persons_found_within_250m_28d,
	t4.persons_found_within_250m_60d,
	t4.persons_found_within_250m_6mo,
	t4.persons_found_within_1km_7d,
	t4.persons_found_within_1km_28d,
	t4.persons_found_within_1km_60d,
	t4.persons_found_within_1km_6mo 
from
	semantic.alerts t1 

right join
	(select * from (
	select 
	datetime_opened::date, 
	count(*) as alert_count from semantic.alerts
	group by datetime_opened::date
	) b
	where b.alert_count < 1000) c on CAST(t1.datetime_opened as DATE) = c.datetime_opened
left join features.weather t2 on CAST(t1.datetime_opened as DATE) = t2.gen_date_weather
left join features.local_authority_metrics t3 on t3.alert = t1.alert	
left join  features.distance_time_from_alert t4 on t4.alert = t1.alert
)


