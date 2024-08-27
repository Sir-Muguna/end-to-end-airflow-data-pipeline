{{
  config(
    materialized='table'
  )
}}
SELECT
  ROW_NUMBER() OVER () AS datetime_id,
  
  -- Convert tpep_pickup_datetime from string to datetime
  CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
  EXTRACT(HOUR FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS pick_hour,
  EXTRACT(DAY FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS pick_day,
  EXTRACT(MONTH FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS pick_month,
  EXTRACT(YEAR FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS pick_year,
  EXTRACT(DAYOFWEEK FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS pick_weekday,
  
  -- Convert tpep_dropoff_datetime from string to datetime
  CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
  EXTRACT(HOUR FROM CAST(tpep_dropoff_datetime AS TIMESTAMP)) AS drop_hour,
  EXTRACT(DAY FROM CAST(tpep_dropoff_datetime AS TIMESTAMP)) AS drop_day,
  EXTRACT(MONTH FROM CAST(tpep_dropoff_datetime AS TIMESTAMP)) AS drop_month,
  EXTRACT(YEAR FROM CAST(tpep_dropoff_datetime AS TIMESTAMP)) AS drop_year,
  EXTRACT(DAYOFWEEK FROM CAST(tpep_dropoff_datetime AS TIMESTAMP)) AS drop_weekday
  
FROM {{ source('uber_data', 'raw_uberdata') }}
