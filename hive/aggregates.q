DROP TABLE IF EXISTS jainpriyansh786.hour_agg;
DROP TABLE IF EXISTS jainpriyansh786.day_agg;
DROP TABLE IF EXISTS jainpriyansh786.dow_agg;
DROP TABLE IF EXISTS jainpriyansh786.month_agg;

-- Hourly aggregations

CREATE TABLE jainpriyansh786.hour_agg
AS 
SELECT hour, day, month, year, dow, AVG(occupancy) as occupancy, SUM(pickup) as pickups, SUM(dropoff) as dropoffs
FROM jainpriyansh786.events 
GROUP BY year, month, dow, day, hour 
ORDER BY hour ASC, day ASC, month ASC, year ASC 


-- Daily aggregations
CREATE TABLE jainpriyansh786.day_agg
AS 
SELECT day, month, year, AVG(occupancy) as occupancy, SUM(pickup) as pickups, SUM(dropoff) as dropoffs
FROM jainpriyansh786.events  
GROUP BY year, month, day 
ORDER BY day ASC, month ASC, year ASC ;


-- Day of Week aggregations
CREATE TABLE jainpriyansh786.dow_agg
AS 
SELECT year, month, dow, hour , AVG(occupancy) as occupancy, AVG(pickups) as pickups, AVG(dropoffs) as dropoffs
FROM jainpriyansh786.hour_agg
GROUP BY year, month, dow , hour
ORDER BY dow ASC, month ASC, year ASC ;

-- Monthly aggregations
CREATE TABLE jainpriyansh786.month_agg
AS 
SELECT year , month, AVG(occupancy) as occupancy, 
SUM(pickup) as pickups, SUM(dropoff) as dropoffs 
FROM jainpriyansh786.events 
GROUP BY year, month 
ORDER BY month ASC, year ASC;