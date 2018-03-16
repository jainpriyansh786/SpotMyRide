drop table if exists jainpriyansh786.idle_duration;
create table if not exists jainpriyansh786.idle_duration(
  
  tripid string,
  cabid string , 
  year int,
  month int , 
  day int , 
  max_duration int

);

insert into table jainpriyansh786.idle_duration 
select a.tripid , a.cabid , a.year , a.month , a.day ,  a.duration from jainpriyansh786.trips as a ,
(select b.cabid, b.year , b.month , b.day ,  max(b.duration) as max_duration
from jainpriyansh786.trips as b where b.idle = 1 group by b.cabid, b.year , b.month , b.day) as b
where a.cabid = b.cabid and a.day = b.day and a.duration = b.max_duration;