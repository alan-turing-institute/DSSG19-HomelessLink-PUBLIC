drop table if exists raw.staging_new_alerts;

create table raw.staging_new_alerts (
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
    )

/* The path below was for testing purposes, should be replaced with filepath
of new streetlink alerts and ran repeatedly, or some other solution reached to
operationalise */
\copy raw.staging_new_alerts from '~/homelesslink/src/etl/incoming_data/simulated_new_data_2019-03-02.csv' header csv delimiter ',';
