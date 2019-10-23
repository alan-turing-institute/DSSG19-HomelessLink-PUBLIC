-- ======
-- Description: 
-- 	1. Creates table for use in QGIS
-- Last Updated: Aug 29, 2019
-- ======

create schema if not exists raw;
create schema if not exists cleaned;
create schema if not exists semantic;
create schema if not exists temp;

create table raw.alerts_processed (
  alert_id varchar,
  alert_number varchar,
  contact_id varchar,
  alert_record_type varchar,
  status varchar,
  duplication_status varchar,
  referral varchar,
  rnumber varchar,
  datetime_opened varchar,
  historic__post_date varchar,
  hour_of_day_alert_raised varchar,
  alert_open_date_formula varchar,
  closed_date varchar,
  historic__report_completed varchar,
  cases_days_open varchar,
  referrer_type varchar,
  alert_origin varchar,
  region varchar,
  local_authority_name varchar,
  group_referral varchar,
  group_number_of_people varchar,
  outcome_summary varchar,
  outcome_internal varchar,
  historic__outcome varchar,
  google_maps_url varchar,
  geo_location_latitude varchar,
  geo_location_longitude varchar,
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
  time_seen_cl varchar,
  time_expected_cl varchar);

create table raw.alerts_processed_geo (
  alert_id varchar,
  alert_number varchar,
  contact_id varchar,
  alert_record_type varchar,
  status varchar,
  duplication_status varchar,
  referral varchar,
  rnumber varchar,
  datetime_opened varchar,
  historic__post_date varchar,
  hour_of_day_alert_raised varchar,
  alert_open_date_formula varchar,
  closed_date varchar,
  historic__report_completed varchar,
  cases_days_open varchar,
  referrer_type varchar,
  alert_origin varchar,
  region varchar,
  local_authority_name varchar,
  group_referral varchar,
  group_number_of_people varchar,
  outcome_summary varchar,
  outcome_internal varchar,
  historic__outcome varchar,
  google_maps_url varchar,
  geo_location_latitude real,
  geo_location_longitude real,
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
  time_seen_cl varchar,
  time_expected_cl varchar);

\copy raw.alerts_processed from '~/../../data/alerts_processed.csv' header csv delimiter ';';
\copy raw.alerts_processed_geo from '~/../../data/alerts_processed.csv' header csv delimiter ';';

alter table raw.alerts_processed_geo add location geography;
insert into raw.alerts_processed_geo (location) select ST_SetSRID(ST_MakePoint(geo_location_longitude, geo_location_latitude), 4326)::geography as location from raw.alerts_processed_geo;
