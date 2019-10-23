-- ======
-- Description: 
-- 	1. Drops if exists features.streetlink_hotspots
-- 	2. Creates table features.streetlink_hotspots based on rows where outcome contains 'hotspot'
-- Last Updated: Aug 29, 2019
-- ======

DROP TABLE IF EXISTS features.streetlink_hotspots;
SELECT * INTO features.streetlink_hotspots FROM (
	select distinct region, location 
	from semantic.alerts 
	where outcome_internal like '%hotspot%'
	) a;
CREATE INDEX streetlink_hotspots_location_ix on 
features.streetlink_hotspots (location);