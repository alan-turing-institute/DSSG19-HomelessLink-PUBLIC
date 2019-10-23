select *
from semantic.alerts
where alert = '5000Y00000V0Y3o' 

select * from semantic.alerts a
where a.datetime_opened >= '2016-12-05'::date - interval '60 days' and
a.datetime_opened < '2016-12-05'
and ST_DWithin('0101000020E6100000AD0782B68B1FB5BFDFB023C44BC34940',a.location, 50)
