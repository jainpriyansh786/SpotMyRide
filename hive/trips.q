DROP TABLE IF EXISTS jainpriyansh786.trips;

CREATE TABLE IF NOT EXISTS jainpriyansh786.trips(
    tripId STRING,
    cabid STRING,
    cab_lat FLOAT,
    cab_long FLOAT,
    occupancy int,
    event_time INT,
    trip INT,
    idle INT,
    duration INT,
    year INT,
    month INT,
    day INT
    
    )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

INSERT INTO TABLE jainpriyansh786.trips

select  concat(cabid,'-',event_time)as tripid , 
cabid , 
cab_lat , 
cab_long , 
occupancy , 
event_time ,
pickup as trip , 
dropoff as idle  ,
(lead(event_time) over (partition by cabid order by event_time) - event_time) as duration,
year(from_unixtime(event_time)) AS year, month(from_unixtime(event_time)) AS month, day(from_unixtime(event_time)) AS day
from jainpriyansh786.events where pickup = 1 or dropoff = 1;