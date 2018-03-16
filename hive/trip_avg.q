DROP TABLE IF EXISTS jainpriyansh786.trip_avg;
CREATE TABLE jainpriyansh786.trip_avg (
    year INT,
    month INT,
    dow STRING,
    avg_dur FLOAT);

-- Find out the average trip duration for a day of week
INSERT INTO TABLE jainpriyansh786.trip_avg 
SELECT year, month, from_unixtime(event_time, 'EEE') as dayofWeek , avg(duration) AS avg_dur
FROM jainpriyansh786.trips where trip = 1 GROUP BY year, month, from_unixtime(event_time, 'EEE');