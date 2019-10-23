-- ======
-- Description: 
-- 	1. Drops if exists the features.distance from nearest hotspot;
-- 	2. Creates various features based on distance from nearest hotspot using cross lateral join 
--	3. Creates index on the alert column so that subsequent joins are fast
-- Last Updated: Aug 29, 2019
-- ======

DROP TABLE IF EXISTS features.distance_from_nearest_hotspot;
SELECT * INTO features.distance_from_nearest_hotspot from (
	select 
	cohort.alert, 
	features.*
	from
		(
		select
		c.alert,
		c.datetime_opened as aod,
		c.location,
		c.region,
		c.outcome_internal
		from semantic.alerts c
		) as cohort
	cross join lateral (
		select
		ST_Distance(a.location,cohort.location) as distance_to_nearest_hotspot,
		(case when ST_Distance(a.location,cohort.location) < 50 then 1 else 0 end) as hotspot_within_50m,
		(case when ST_Distance(a.location,cohort.location) < 100 then 1 else 0 end) as hotspot_within_100m,
		(case when ST_Distance(a.location,cohort.location) < 250 then 1 else 0 end) as hotspot_within_250m,
		(case when ST_Distance(a.location,cohort.location) < 500 then 1 else 0 end) as hotspot_within_500m,
		(case when ST_Distance(a.location,cohort.location) < 1000 then 1 else 0 end) as hotspot_within_1000m,
		(case when ST_Distance(a.location,cohort.location) < 5000 then 1 else 0 end) as hotspot_within_5000m,
		(case when ST_Distance(a.location,cohort.location) < 10000 then 1 else 0 end) as hotspot_within_10km
		from features.streetlink_hotspots a
		ORDER BY a.location <-> cohort.location
		LIMIT 1
	) as features 
) b;

CREATE INDEX distance_from_nearest_hotspot_alert_ix on features.distance_from_nearest_hotspot (alert);
