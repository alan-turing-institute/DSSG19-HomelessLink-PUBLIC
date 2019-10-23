SELECT
pid,
now() - query_start as duration,
query,
state
from pg_stat_activity
order by duration desc
