select 
client,
source,
medium,
max(platform_n) platform,
max(channel_n) channel,
time_of_entry
from  ( 

SELECT  
client,
source,
medium,
platform as platform_n,
channel as channel_n,
time_of_entry,
first_value(time_of_entry) OVER (PARTITION BY client, source, medium ORDER BY time_of_entry DESC) lv
FROM {{ var("mappings_ga") }}

) 

WHERE lv = time_of_entry
and client in (select client from {{ref('accounts_proc')}} )
group by client, source, medium, platform_n, channel_n, time_of_entry
order by client asc, source asc