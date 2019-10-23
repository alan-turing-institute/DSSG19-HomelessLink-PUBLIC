-- ======
-- Description: 
-- 	1. Creates raw, cleaned, and semantic schemas 
-- 	2. Creates raw.alerts_raw table
--	3. Inserts csv data into raw.alerts_raw 
-- NOTE: /copy is a psql command, so this that portion should be run in psql client
-- Last Updated: Aug 29, 2019
-- ======

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS cleaned;
CREATE SCHEMA IF NOT EXISTS semantic;
CREATE SCHEMA IF NOT EXISTS temp;
CREATE SCHEMA IF NOT EXISTS features;

CREATE TABLE raw.alerts_raw (
alert_id varchar,
alert_number varchar,
contact_id varchar,
alert_record_type varchar,
status varchar,
duplication_status varchar,
referral varchar,
referral_id varchar,
datetime_opened varchar,
historic_post_date varchar,
hour_of_day_alert_raised varchar,
alert_open_date_formula varchar,
closed_date varchar,
historic_reported_completed varchar,
cases_days_open varchar,
referrer_type varchar,
alert_origin varchar,
region varchar,
local_authority_name varchar,
group_referral varchar,
group_number_of_people varchar,
outcome_summary varchar,
outcome_internal varchar,
historic_outcome varchar,
google_maps_url varchar,
geo_latitude varchar,
geo_longitude varchar,
time_seen varchar,
typical_time_seen_at_this_site varchar,
time_expected varchar,
street varchar,
postcode varchar,
location_details varchar,
age_group varchar,
gender varchar,
appearance varchar,
immediate_concerns_about_client varchar,
contact_consent varchar,
additional_information_requested_on_date varchar,
additional_information_received_on_date varchar,
sl_legacy_id varchar,
parent_alert_number varchar,
parent_alert_id varchar
);

--\copy raw.alerts_raw from 'xxxxx.csv' header csv delimiter ',';
