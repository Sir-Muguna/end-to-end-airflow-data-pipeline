{{ config(materialized='table') }}

WITH raw_data AS (
  SELECT
    VendorID,     
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime, 
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,     
    passenger_count,     
    trip_distance,     
    pickup_longitude,     
    pickup_latitude,    
    RatecodeID,    
    store_and_fwd_flag,      
    dropoff_longitude,    
    dropoff_latitude,     
    payment_type,     
    fare_amount,     
    extra,    
    mta_tax,     
    tip_amount,    
    tolls_amount,     
    improvement_surcharge,    
    total_amount
  FROM {{ source('uber_data', 'raw_uberdata') }} AS r
),

datetime_dim AS (
  SELECT
    datetime_id,
    tpep_pickup_datetime
  FROM {{ ref('datetime_dim') }} AS d
),

passenger_count_dim AS (
  SELECT
    passenger_count_id,
    passenger_count
  FROM {{ ref('passenger_count_dim') }} AS pc
),

trip_distance_dim AS (
  SELECT
    trip_distance_id,
    trip_distance
  FROM {{ ref('trip_distance_dim') }} AS td
),

rate_code_dim AS (
  SELECT
    rate_code_id,
    RatecodeID
  FROM {{ ref('rate_code_dim') }} AS rc
),

pickup_location_dim AS (
  SELECT
    pickup_location_id,
    pickup_latitude,
    pickup_longitude
  FROM {{ ref('pickup_location_dim') }} AS pl
),

dropoff_location_dim AS (
  SELECT
    dropoff_location_id,
    dropoff_latitude,
    dropoff_longitude
  FROM {{ ref('dropoff_location_dim') }} AS dl
),

payment_type_dim AS (
  SELECT
    payment_type_id,
    payment_type
  FROM {{ ref('payment_type_dim') }} AS pt
)

SELECT
  r.VendorID,
  d.datetime_id,
  pc.passenger_count_id,
  td.trip_distance_id,
  rc.rate_code_id,
  pl.pickup_location_id,
  dl.dropoff_location_id,
  pt.payment_type_id,
  r.store_and_fwd_flag,
  r.fare_amount,
  r.extra,
  r.mta_tax,
  r.tip_amount,
  r.tolls_amount,
  r.improvement_surcharge,
  r.total_amount

FROM raw_data AS r
LEFT JOIN datetime_dim AS d
  ON r.tpep_pickup_datetime = d.tpep_pickup_datetime
LEFT JOIN passenger_count_dim AS pc
  ON r.passenger_count = pc.passenger_count
LEFT JOIN trip_distance_dim AS td
  ON r.trip_distance = td.trip_distance
LEFT JOIN rate_code_dim AS rc
  ON r.RatecodeID = rc.RatecodeID
LEFT JOIN pickup_location_dim AS pl
  ON r.pickup_latitude = pl.pickup_latitude
  AND r.pickup_longitude = pl.pickup_longitude
LEFT JOIN dropoff_location_dim AS dl
  ON r.dropoff_latitude = dl.dropoff_latitude
  AND r.dropoff_longitude = dl.dropoff_longitude
LEFT JOIN payment_type_dim AS pt
  ON r.payment_type = pt.payment_type
