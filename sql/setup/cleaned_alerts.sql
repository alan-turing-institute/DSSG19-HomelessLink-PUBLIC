-- ======
-- Description: 
-- 	1. Drops if exists cleaned.alerts
-- 	2. Creates cleaned.alerts, removing nulls, casting columns to the correct types, etc.
-- Last Updated: Aug 29, 2019
-- ======

DROP TABLE IF EXISTS cleaned.alerts;
CREATE TABLE if not exists cleaned.alerts AS (
SELECT
	alert_id::varchar as alert,
	alert_number::integer as alert_external,
	NULLIF(contact_id,'')::varchar as contact,
	NULLIF(btrim(lower(alert_record_type)),'')::varchar as alert_record_type,
	btrim(lower(status))::varchar as alert_status,
	NULLIF(btrim(lower(duplication_status)),'')::varchar as duplication_status,
	NULLIF(referral_id,'')::varchar as referral,
	case
		when historic_post_date = ''
			then to_timestamp(datetime_opened, 'DD/MM/YYYY HH24:MI')
		else to_timestamp(concat(historic_post_date, ' 00:00'), 'DD/MM/YYYY HH24:MI')
		end as datetime_opened,
	case
		when historic_reported_completed = '' and closed_date = ''
			then NULL
		when historic_reported_completed = ''
			then to_date(closed_date, 'DD/MM/YYYY')
		else to_date(historic_reported_completed, 'DD/MM/YYYY')
		end as date_closed,
	NULLIF(btrim(lower(referrer_type)),'')::varchar as notifier_type,
	NULLIF(btrim(lower(alert_origin)),'')::varchar as alert_origin,
	NULLIF(btrim(lower(region)),'')::varchar as region,
	NULLIF(btrim(lower(local_authority_name)),'')::varchar as local_authority,
	NULLIF(btrim(group_referral),'')::varchar as group_referral,
	NULLIF(group_number_of_people,'')::smallint as group_number_of_people,
	NULLIF(btrim(lower(outcome_summary)),'')::varchar as outcome_summary,
	NULLIF(btrim(lower(outcome_internal)),'')::varchar as outcome_internal,
	NULLIF(btrim(lower(historic_outcome)),'')::varchar as historic_outcome,
	NULLIF(btrim(lower(google_maps_url)),'')::varchar as google_maps_url,
	ST_SetSRID(ST_MakePoint(NULLIF(geo_longitude,'')::double precision, NULLIF(geo_latitude,'')::double precision), 4326)::geography as location,
	substring(
		CASE WHEN time_seen ilike '%p%'
			THEN (substring(time_seen,'[0-9]+')::integer + 12)::varchar
			ELSE time_seen
			END ,'[0-9]{1,2}') as time_seen,
	case
		when typical_time_seen_at_this_site in ('')
		then FALSE else TRUE end as typical_time_seen_flag,
	substring(
		CASE WHEN time_expected ilike '%p%'
			THEN (substring(time_expected,'[0-9]+')::integer + 12)::varchar
			ELSE time_expected 
			END ,'[0-9]{1,2}') as time_expected,
	NULLIF(btrim(lower(street)),'')::varchar as street,
	NULLIF(btrim(lower(postcode)),'')::varchar as postcode,
	NULLIF(btrim(regexp_replace(lower(location_details),'\\s+', ' ','g')),'')::varchar as location_details,
	NULLIF(btrim(lower(age_group)),'')::varchar as age_group,
	NULLIF(btrim(lower(gender)),'')::varchar as gender,
	NULLIF(btrim(regexp_replace(lower(appearance),'\\s+', ' ','g')),'')::varchar as appearance_details,
	NULLIF(btrim(regexp_replace(lower(immediate_concerns_about_client),'\\s+', ' ','g')),'')::varchar as immediate_concerns_details,
	NULLIF(contact_consent,'')::bool as contact_consent,
	to_timestamp(NULLIF(additional_information_requested_on_date,''), 'DD/MM/YYYY HH24:MI') as date_info_requested,
	to_timestamp(NULLIF(additional_information_received_on_date,''), 'DD/MM/YYYY HH24:MI') as date_info_received,
	NULLIF(btrim(sl_legacy_id),'')::varchar as sl_legacy,
	NULLIF(btrim(parent_alert_id),'')::varchar as parent_alert,
	NULLIF(btrim(parent_alert_number)::integer,0) as parent_alert_number
from raw.alerts_raw
)
