DROP TABLE IF EXISTS hbase_dow_table;

CREATE TABLE IF NOT EXISTS hbase_dow_table(
    rowkey STRING,
    hour STRING,
    occupancy FLOAT,
    pickups FLOAT,
    dropoffs FLOAT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, c:hour, c:occ, c:pickups, c:dropoffs")
;

INSERT OVERWRITE TABLE hbase_dow_table SELECT CONCAT(year,'_',month,'_',dow, '-',hour), hour, occupancy, pickups, dropoffs FROM dow_agg;
