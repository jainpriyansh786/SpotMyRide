drop table if exists jainpriyansh786.events ; 
create table if not exists jainpriyansh786.events
(

cabid string ,
cab_lat float ,
cab_long float ,
occupancy int ,
event_time int,
pickup int,
dropoff int ,
year int,
month int,
day int ,
hour int ,
dow string
  
);


Insert overwrite table jainpriyansh786.events
select  cabid , cab_lat , cab_long , occupancy , 
event_time , 
case when flip = -1 then 1 else 0 end as pickup,
case when flip = 1 then 1 else 0 end as dropoff,
year(from_unixtime(event_time)) AS year, 
month(from_unixtime(event_time)) AS month,
day(from_unixtime(event_time)) AS day, 
hour(from_unixtime(event_time)) AS hour, 
from_unixtime(event_time, 'EEE') as dow
from 
(select cabid , cab_lat , cab_long , occupancy , event_time , lag(occupancy,1,0) over(partition by cabid order by event_time) - occupancy as flip 

from jainpriyansh786.cab_data) as x;