select 
	a.local_authority,
	a.datetime_opened,
	count(*) as alert_count
from cleaned.alerts a

group by 
	a.local_authority,
	a.datetime_opened

order by 
	a.local_authority asc, 
	a.datetime_opened asc