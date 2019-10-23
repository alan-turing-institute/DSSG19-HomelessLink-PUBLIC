-- ======
-- Description: 
-- 	1. Drops if exists features.local_authority_metrics
-- 	2. Creates various features based on the local authority. More dimension details below. 
-- 		2a. Response time: Average time that it takes for a local authority to close a case (i.e. report an outcome) based on datetime_opened and date_closed
-- 		2b. Alerts, referrals, and persons found counts
-- 		2c. Person found rate: Persons found count over referral counts
--		2d. Time: Last 7 days, 28 days
--	3. Creates index on the alert and datetime_opened column so that joins with other tables are fast
-- Last Updated: Aug 29, 2019
-- ======

DROP TABLE IF EXISTS features.local_authority_metrics;
SELECT * INTO features.local_authority_metrics from (
	select cohort.alert, features.*
	from (
		select
		c.alert, c.datetime_opened as aod, c.local_authority
		from semantic.alerts c
		) as cohort
	inner join lateral (
		select

		-- la response time in past X days
		COALESCE(avg(a.date_closed::date - a.datetime_opened::date)
		filter (where a.datetime_opened >= cohort.aod::date - interval '7 days' and a.datetime_opened::date < cohort.aod::date)::int,0)
		as la_avg_response_time_7d,
		COALESCE(avg(a.date_closed::date - a.datetime_opened::date)
		filter (where a.datetime_opened >= cohort.aod::date - interval '28 days' and a.datetime_opened::date < cohort.aod::date)::int,0)
		as la_avg_response_time_28d,

		-- alert count features
		count(a.alert)
		filter (where a.datetime_opened >= cohort.aod::date - interval '7 days' and a.datetime_opened::date < cohort.aod::date)
		as alerts_last_7d,
		count(a.alert)
		filter (where a.datetime_opened >= cohort.aod::date - interval '28 days' and a.datetime_opened::date < cohort.aod::date)
		as alerts_last_28d,

		-- person found features
		COALESCE(sum(a.person_found_flag)
		filter (where a.datetime_opened >= cohort.aod::date - interval '7 days' and a.datetime_opened::date < cohort.aod::date),0)
		as person_found_last_7d,
		COALESCE(sum(a.person_found_flag)
		filter (where a.datetime_opened >= cohort.aod::date - interval '28 days' and a.datetime_opened::date < cohort.aod::date),0)
		as person_found_last_28d,

		-- referral features
		COALESCE(sum(a.referral_flag)
		filter (where a.datetime_opened >= cohort.aod::date - interval '7 days' and a.datetime_opened::date < cohort.aod::date),0)
		as referrals_last_7d,
		COALESCE(sum(a.referral_flag)
		filter (where a.datetime_opened >= cohort.aod::date - interval '28 days' and a.datetime_opened::date < cohort.aod::date),0)
		as referrals_last_28d,

		-- person found rate in last x days
		COALESCE(round(sum(a.person_found_flag)
		filter (where a.datetime_opened >= cohort.aod::date - interval '7 days' and a.datetime_opened::date < cohort.aod::date)::decimal(5,2) /
		NULLIF(
		sum(a.referral_flag)
		filter (where a.datetime_opened >= cohort.aod::date - interval '7 days' and a.datetime_opened::date < cohort.aod::date)::decimal(5,2),0),2)
		,0) as person_found_rate_last_7d,
		COALESCE(round(sum(a.person_found_flag)
		filter (where a.datetime_opened >= cohort.aod::date - interval '28 days' and a.datetime_opened::date < cohort.aod::date)::decimal(5,2) /
		NULLIF(
		sum(a.referral_flag)
		filter (where a.datetime_opened >= cohort.aod::date - interval '28 days' and a.datetime_opened::date < cohort.aod::date)::decimal(5,2),0) ,2)
		,0) as person_found_rate_last_28d
		from (
			select *, 
			case when outcome_summary like '%person found%' then 1 
			else 0 end 
			as person_found_flag 
			from semantic.alerts
			) a
		where a.local_authority = cohort.local_authority
		group by a.local_authority
	) as features on true
) b;

CREATE INDEX local_authority_metrics_alert_ix on features.local_authority_metrics (alert);
CREATE INDEX local_authority_metrics_datetime_opened_ix on features.local_authority_metrics (datetime_opened);
