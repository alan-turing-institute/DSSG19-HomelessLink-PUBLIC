-- ======
-- Description: 
-- 	1. Drops if exists features.distance_time_from_alert
-- 	2. Creates various features based on the count of alerts, referrals, and persons found at various lookback windows & distances. More details on dimensions below.
-- 		2a. State of alert: Alerts x referrals x persons found
-- 		2b. Alert origin: Mobile app x phone x website
-- 		2c. Time: 7 days, 28 days, 60 days 6 months
--		2d. Distance: 50m, 250m, 1000m, 5000m
--	3. Creates index on the alert column so that subsequent joins are fast
-- Last Updated: Aug 29, 2019
-- ======

DROP TABLE IF EXISTS features.distance_time_from_alert;
SELECT * INTO features.distance_time_from_alert from (
select cohort.alert, features.*
from
	(
	select
	c.alert,
	c.datetime_opened as aod,
	c.location,
	c.region
	from semantic.alerts c
	) as cohort
inner join lateral (
	select
	-- alert-level 50m features
	count(a.alert)
		filter (
		where a.datetime_opened >= cohort.aod::date - interval '7 days'
		and a.datetime_opened::date < cohort.aod::date
		and ST_DWithin(cohort.location, a.location, 50)
		)
	as alerts_within_50m_7d,
  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    and a.alert_origin = 'web'
    )
  as alerts_within_50m_7d_web_origin,
  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    and a.alert_origin = 'phone'
    )
  as alerts_within_50m_7d_phone_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    and a.alert_origin = 'mobile app'
    )
  as alerts_within_50m_7d_mobile_app_origin,

	count(a.alert)
		filter (
		where a.datetime_opened >= cohort.aod::date - interval '28 days'
		and a.datetime_opened::date < cohort.aod::date
		and ST_DWithin(cohort.location, a.location, 50)
		) as alerts_within_50m_28d,

    count(a.alert)
      filter (
      where a.datetime_opened >= cohort.aod::date - interval '28 days'
      and a.datetime_opened::date < cohort.aod::date
      and ST_DWithin(cohort.location, a.location, 50)
      and a.alert_origin = 'web'
      )
    as alerts_within_50m_28d_web_origin,

    count(a.alert)
      filter (
      where a.datetime_opened >= cohort.aod::date - interval '28 days'
      and a.datetime_opened::date < cohort.aod::date
      and ST_DWithin(cohort.location, a.location, 50)
      and a.alert_origin = 'phone'
      )
    as alerts_within_50m_28d_phone_origin,

    count(a.alert)
      filter (
      where a.datetime_opened >= cohort.aod::date - interval '28 days'
      and a.datetime_opened::date < cohort.aod::date
      and ST_DWithin(cohort.location, a.location, 50)
      and a.alert_origin = 'mobile app'
      )
    as alerts_within_50m_28d_mobile_app_origin,

	count(a.alert)
		filter (
		where a.datetime_opened >= cohort.aod::date - interval '60 days'
		and a.datetime_opened::date < cohort.aod::date
		and ST_DWithin(cohort.location, a.location, 50)
		)
	as alerts_within_50m_60d,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    and a.alert_origin = 'web'
    )
  as alerts_within_50m_60d_web_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    and a.alert_origin = 'phone'
    )
  as alerts_within_50m_60d_phone_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    and a.alert_origin = 'mobile app'
    )
  as alerts_within_50m_60d_mobile_app_origin,


	count(a.alert)
		filter (
		where a.datetime_opened >= cohort.aod::date - interval '6 months'
		and a.datetime_opened::date < cohort.aod::date
		and ST_DWithin(cohort.location, a.location, 50)
		)
	as alerts_within_50m_6mo,


  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    and a.alert_origin = 'web'
    )
  as alerts_within_50m_6mo_web_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    and a.alert_origin = 'phone'
    )
  as alerts_within_50m_6mo_phone_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    and a.alert_origin = 'mobile app'
    )
  as alerts_within_50m_6mo_mobile_app_origin,

-- alert-level 250m features

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  )
as alerts_within_250m_7d,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  and a.alert_origin = 'web'
  )
as alerts_within_250m_7d_web_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  and a.alert_origin = 'phone'
  )
as alerts_within_250m_7d_phone_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  and a.alert_origin = 'mobile app'
  )
as alerts_within_250m_7d_mobile_app_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  ) as alerts_within_250m_28d,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 250)
    and a.alert_origin = 'web'
    )
  as alerts_within_250m_28d_web_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 250)
    and a.alert_origin = 'phone'
    )
  as alerts_within_250m_28d_phone_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 250)
    and a.alert_origin = 'mobile app'
    )
  as alerts_within_250m_28d_mobile_app_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  )
as alerts_within_250m_60d,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  and a.alert_origin = 'web'
  )
as alerts_within_250m_60d_web_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  and a.alert_origin = 'phone'
  )
as alerts_within_250m_60d_phone_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  and a.alert_origin = 'mobile app'
  )
as alerts_within_250m_60d_mobile_app_origin,


count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  )
as alerts_within_250m_6mo,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  and a.alert_origin = 'web'
  )
as alerts_within_250m_6mo_web_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  and a.alert_origin = 'phone'
  )
as alerts_within_250m_6mo_phone_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  and a.alert_origin = 'mobile app'
  )
as alerts_within_250m_6mo_mobile_app_origin,

-- alert-level 1km features
count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  )
as alerts_within_1000m_7d,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  and a.alert_origin = 'web'
  )
as alerts_within_1000m_7d_web_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  and a.alert_origin = 'phone'
  )
as alerts_within_1000m_7d_phone_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  and a.alert_origin = 'mobile app'
  )
as alerts_within_1000m_7d_mobile_app_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  ) as alerts_within_1000m_28d,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    and a.alert_origin = 'web'
    )
  as alerts_within_1000m_28d_web_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    and a.alert_origin = 'phone'
    )
  as alerts_within_1000m_28d_phone_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    and a.alert_origin = 'mobile app'
    )
  as alerts_within_1000m_28d_mobile_app_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  )
as alerts_within_1000m_60d,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  and a.alert_origin = 'web'
  )
as alerts_within_1000m_60d_web_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location,1000)
  and a.alert_origin = 'phone'
  )
as alerts_within_1000m_60d_phone_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  and a.alert_origin = 'mobile app'
  )
as alerts_within_1000m_60d_mobile_app_origin,


count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  )
as alerts_within_1000m_6mo,


count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  and a.alert_origin = 'web'
  )
as alerts_within_1000m_6mo_web_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  and a.alert_origin = 'phone'
  )
as alerts_within_1000m_6mo_phone_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 1000)
  and a.alert_origin = 'mobile app'
  )
as alerts_within_1000m_6mo_mobile_app_origin,

-- alert-level 5000m features
count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  )
as alerts_within_5000m_7d,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  and a.alert_origin = 'web'
  )
as alerts_within_5000m_7d_web_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  and a.alert_origin = 'phone'
  )
as alerts_within_5000m_7d_phone_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  and a.alert_origin = 'mobile app'
  )
as alerts_within_5000m_7d_mobile_app_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  ) as alerts_within_5000m_28d,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    and a.alert_origin = 'web'
    )
  as alerts_within_5000m_28d_web_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    and a.alert_origin = 'phone'
    )
  as alerts_within_5000m_28d_phone_origin,

  count(a.alert)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    and a.alert_origin = 'mobile app'
    )
  as alerts_within_5000m_28d_mobile_app_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  )
as alerts_within_5000m_60d,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  and a.alert_origin = 'web'
  )
as alerts_within_5000m_60d_web_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location,5000)
  and a.alert_origin = 'phone'
  )
as alerts_within_5000m_60d_phone_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  and a.alert_origin = 'mobile app'
  )
as alerts_within_5000m_60d_mobile_app_origin,


count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  )
as alerts_within_5000m_6mo,


count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  and a.alert_origin = 'web'
  )
as alerts_within_5000m_6mo_web_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  and a.alert_origin = 'phone'
  )
as alerts_within_5000m_6mo_phone_origin,

count(a.alert)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 5000)
  and a.alert_origin = 'mobile app'
  )
as alerts_within_5000m_6mo_mobile_app_origin,

-- referral-level 50m features
COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_7d,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'web'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_7d_web_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'phone'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_7d_phone_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'mobile app'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_7d_mobile_app_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_28d,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'web'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_28d_web_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'phone'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_28d_phone_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'mobile app'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_28d_mobile_app_origin,


COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_60d,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'web'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_60d_web_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'phone'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_60d_phone_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'mobile app'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_60d_mobile_app_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_6mo,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'web'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_6mo_web_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'phone'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_6mo_phone_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'mobile app'
  and ST_DWithin(cohort.location, a.location, 50)
  ),0)
as referrals_within_50m_6mo_mobile_app_origin,
-- referral 250m features

-- referral-level 250m features
COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_7d,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'web'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_7d_web_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'phone'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_7d_phone_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '7 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'mobile app'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_7d_mobile_app_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_28d,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'web'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_28d_web_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'phone'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_28d_phone_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '28 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'mobile app'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_28d_mobile_app_origin,


COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_60d,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'web'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_60d_web_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'phone'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_60d_phone_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '60 days'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'mobile app'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_60d_mobile_app_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_6mo,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'web'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_6mo_web_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'phone'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_6mo_phone_origin,

COALESCE(sum(a.referral_flag)
  filter (
  where a.datetime_opened >= cohort.aod::date - interval '6 months'
  and a.datetime_opened::date < cohort.aod::date
  and a.alert_origin = 'mobile app'
  and ST_DWithin(cohort.location, a.location, 250)
  ),0)
as referrals_within_250m_6mo_mobile_app_origin,

  -- referral-level 1000m features
  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_7d,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_7d_web_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_7d_phone_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_7d_mobile_app_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_28d,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_28d_web_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_28d_phone_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_28d_mobile_app_origin,


  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_60d,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_60d_web_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_60d_phone_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_60d_mobile_app_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_6mo,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_6mo_web_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_6mo_phone_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as referrals_within_1000m_6mo_mobile_app_origin,

  -- referral-level 5000m features
  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_7d,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_7d_web_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_7d_phone_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_7d_mobile_app_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_28d,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_28d_web_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_28d_phone_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_28d_mobile_app_origin,


  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_60d,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_60d_web_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_60d_phone_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_60d_mobile_app_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_6mo,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_6mo_web_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_6mo_phone_origin,

  COALESCE(sum(a.referral_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as referrals_within_5000m_6mo_mobile_app_origin,
  
  -- person found-level 50m features
  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_7d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_7d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_7d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_7d_mobile_app_origin,


  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_28d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_28d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_28d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_28d_mobile_app_origin,


  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_60d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_60d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_60d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_60d_mobile_app_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_6mo,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_6mo_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_6mo_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 50)
    ),0)
  as persons_found_within_50m_6mo_mobile_app_origin,

	-- person found-level 250m features
  -- person found-level 250m features
  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_7d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_7d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_7d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_7d_mobile_app_origin,


  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_28d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_28d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_28d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_28d_mobile_app_origin,


  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_60d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_60d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_60d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_60d_mobile_app_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_6mo,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_6mo_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_6mo_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 250)
    ),0)
  as persons_found_within_250m_6mo_mobile_app_origin,

	-- person found 1km features

  -- person found-level 1000m features
  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_7d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_7d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_7d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_7d_mobile_app_origin,


  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_28d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_28d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_28d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_28d_mobile_app_origin,


  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_60d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_60d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_60d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_60d_mobile_app_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_6mo,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_6mo_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_6mo_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 1000)
    ),0)
  as persons_found_within_1000m_6mo_mobile_app_origin,

  -- person found 5km features
  -- person found-level 5000m features
  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_7d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_7d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_7d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '7 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_7d_mobile_app_origin,


  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_28d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_28d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_28d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '28 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_28d_mobile_app_origin,


  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_60d,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_60d_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_60d_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '60 days'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_60d_mobile_app_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_6mo,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'web'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_6mo_web_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'phone'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_6mo_phone_origin,

  COALESCE(sum(a.person_found_flag)
    filter (
    where a.datetime_opened >= cohort.aod::date - interval '6 months'
    and a.datetime_opened::date < cohort.aod::date
    and a.alert_origin = 'mobile app'
    and ST_DWithin(cohort.location, a.location, 5000)
    ),0)
  as persons_found_within_5000m_6mo_mobile_app_origin

	from
	(select *, case when outcome_summary like '%person found%' then 1 else 0 end as person_found_flag from semantic.alerts) a
	where ST_DWithin(a.location,cohort.location,0)
	group by a.location
) as features on true
) b;

CREATE INDEX distance_time_from_alert_alert_ix on features.distance_time_from_alert (alert);
