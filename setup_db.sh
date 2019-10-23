service=$(grep DBNAME .env | cut -d = -f 2)
psql service=$service -f create_extensions.sql
psql service=$service -f raw_preprocessed_alerts.sql
psql service=$service -f raw_weather.sql
psql service=$service -f cleaned_alerts.sql
psql service=$service -f cleaned_weather.sql
psql service=$service -f semantic_alerts.sql
psql service=$service -f semantic_weather.sql
psql service=$service -f features_weather.sql
psql service=$service -f create_features_hotspots_list.sql
psql service=$service -f features_local_authority.sql
psql service=$service -f features_distance_from_nearest_hotspot.sql
psql service=$service -f features_distance_time_from_alert.sql
psql service=$service -f join_features_with_semantic_alerts.sql
psql service=$service -f results_tables.sql
