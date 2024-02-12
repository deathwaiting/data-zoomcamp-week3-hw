-------------------------------------------------------------------------------
-- Question 1 : create external table from multiple parquet files and a materialized table
CREATE OR REPLACE EXTERNAL TABLE `qu-qu-353611.demo_dataset.green_tripdata_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://qu_qu_353611_terraform-demo-terra-bucket/green-taxi-2022/green_tripdata_2022-*.parquet']
);

create or replace table `qu-qu-353611.demo_dataset.green_tripdata_materialized` AS
select * from `qu-qu-353611.demo_dataset.green_tripdata_external`;


-------------------------------------------------------------------------------
-- Question 2 : count distinct PULocationID on external and materialized tables
select count(*) from (
  select distinct PULocationID from `qu-qu-353611.demo_dataset.green_tripdata_external`
);

select count(*) from (
  select distinct PULocationID from `qu-qu-353611.demo_dataset.green_tripdata_materialized`
);


-------------------------------------------------------------------------------
-- Question 3 : How many records have a fare_amount of 0?
select count(*) from `qu-qu-353611.demo_dataset.green_tripdata_materialized`
where fare_amount = 0;

-------------------------------------------------------------------------------
-- Question 4 : partion by lpep_pickup_datetime as date and Cluster on PUlocationID for ordering
create or replace table `qu-qu-353611.demo_dataset.green_tripdata_partitioned` 
partition by date(lpep_pickup_datetime)
CLUSTER BY PUlocationID
as 
(select * from `qu-qu-353611.demo_dataset.green_tripdata_materialized`);

-------------------------------------------------------------------------------
-- Question 5 : a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
select distinct PULocationID from `qu-qu-353611.demo_dataset.green_tripdata_materialized`
where DATE(lpep_pickup_datetime) between PARSE_DATE('%m/%d/%Y', '06/01/2022') and PARSE_DATE('%m/%d/%Y', '06/30/2022');

select distinct PULocationID from `qu-qu-353611.demo_dataset.green_tripdata_partitioned`
where DATE(lpep_pickup_datetime) between PARSE_DATE('%m/%d/%Y', '06/01/2022') and PARSE_DATE('%m/%d/%Y', '06/30/2022');