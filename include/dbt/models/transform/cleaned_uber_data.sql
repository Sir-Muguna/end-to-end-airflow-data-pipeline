{{
  config(
    materialized='table'
  )
}}

WITH cleaned_uber_data AS (
  SELECT
    CAST(VendorID AS INTEGER) AS VendorID,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS FLOAT64) AS trip_distance,
    CAST(pickup_longitude AS FLOAT64) AS pickup_longitude,
    CAST(pickup_latitude AS FLOAT64) AS pickup_latitude,
    CAST(RatecodeID AS INTEGER) AS RatecodeID,
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
    CAST(dropoff_longitude AS FLOAT64) AS dropoff_longitude,
    CAST(dropoff_latitude AS FLOAT64) AS dropoff_latitude,
    CAST(payment_type AS INTEGER) AS payment_type,
    CAST(fare_amount AS FLOAT64) AS fare_amount,
    CAST(extra AS FLOAT64) AS extra,
    CAST(mta_tax AS FLOAT64) AS mta_tax,
    CAST(tip_amount AS FLOAT64) AS tip_amount,
    CAST(tolls_amount AS FLOAT64) AS tolls_amount,
    CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
    CAST(total_amount AS FLOAT64) AS total_amount
  FROM {{ source('uber_data', 'raw_uberdata') }} AS r
)

SELECT * FROM cleaned_uber_data
