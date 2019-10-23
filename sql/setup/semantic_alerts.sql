-- ======
-- Description: 
-- 	1. Drops if exists semantic.alerts table
-- 	2. Selects columns from cleaned.alerts and appends them with additional features
--	3. Generates index on table so subsequent queries on this table run more quickly.
-- Last Updated: Aug 29, 2019
-- ======

DROP TABLE IF EXISTS semantic.alerts;
CREATE TABLE IF NOT EXISTS semantic.alerts
as	(
SELECT *,
-- ADDS FEATURES
concat(location_details, ',', appearance_details) as loc_app_details,
cosd(360 * extract(dow from datetime_opened) / 7) as dowx,
sind(360 * extract(dow from datetime_opened) / 7) as dowy,
cosd(360 * extract(day from datetime_opened) / date_part('days', date_trunc('month', datetime_opened) + interval '1 month' - interval '1 day')) as domx,
sind(360 * extract(day from datetime_opened) / date_part('days', date_trunc('month', datetime_opened) + interval '1 month' - interval '1 day')) as domy,
-- Fix for leap years?
cosd(360 * extract(doy from datetime_opened) / 365) as doyx,
sind(360 * extract(doy from datetime_opened) / 365) as doyy,
extract(day from date_trunc('day', datetime_opened - '2012-10-01')) as date_linear,
case when time_seen is not null then 1 else 0 end as time_seen_flag,
case when time_expected is not null then 1 else 0 end as time_expected_flag,
-- email will be our reference group for alert origin
case when alert_origin like '%app%' then 1 else 0 end as alert_origin_mobile,
case when alert_origin = 'web' then 1 else 0 end as alert_origin_web,
case when alert_origin = 'phone' then 1 else 0 end as alert_origin_phone,
-- gender NULL will be our reference group
case when gender = 'female' then 1 else 0 end as gender_female,
case when gender = 'male' then 1 else 0 end as gender_male,
case when gender = 'unknown' then 1 else 0 end as gender_unknown,
case when gender = 'non-binary' then 1 else 0 end as gender_nonbinary,
case when group_referral is not null then 1 else 0 end as group_referral_flag,
-- reference group is self-referrals
case when notifier_type = 'member of the public' then 1 else 0 end as member_of_public_flag,
case when appearance_details is not null then 1 else 0 end as appearance_details_entry_flag,
case when location_details is not null then 1 else 0 end as location_details_entry_flag,
CASE WHEN outcome_internal in ('person found - engaging with services', 'person found - no entitlement to local services',
'person found - reconnected to services in other areas', 'person found - street activity/begging', 'person found - referred to emergency services',
'person found - taken to shelter', 'person found - unwilling to engage', 'la did not respond', 'person not found - no outreach service in this area',
'person not found - person looked for', 'person not found - person no longer rough sleeping', 'person not found - site could not be located',
'person not found - site not accessible' , 'person not looked for - person already known', 'person not looked for - street activity/begging location',
'no action taken - identified hotspot', 'person not looked for - wrong local authority / geographical information')
THEN 1
ELSE 0
END as referral_flag,

--word count for appearance description
CASE WHEN ((LENGTH(appearance_details) - LENGTH(replace(appearance_details, ' ', ''))) + 1) > 0
THEN ((LENGTH(appearance_details) - LENGTH(replace(appearance_details, ' ', ''))) + 1)
ELSE 0
END as appearance_wordcount,

--word count for location description
CASE WHEN ((LENGTH(location_details) - LENGTH(replace(location_details, ' ', ''))) + 1) > 0
THEN ((LENGTH(location_details) - LENGTH(replace(location_details, ' ', ''))) + 1)
ELSE 0
END as location_wordcount,

-- word count for immediate concerns
CASE WHEN ((LENGTH(immediate_concerns_details) - LENGTH(replace(immediate_concerns_details, ' ', ''))) + 1) > 0
THEN ((LENGTH(immediate_concerns_details) - LENGTH(replace(immediate_concerns_details, ' ', ''))) + 1)
ELSE 0
END as concerns_wordcount

FROM cleaned.alerts as t1
where t1.local_authority is not null
and t1.location is not null

);

CREATE INDEX alerts_alert_ix on semantic.alerts (alert);
CREATE INDEX alerts_datetime_opened_ix on semantic.alerts (datetime_opened);
CREATE INDEX alerts_location_gix on semantic.alerts USING GIST (location);
