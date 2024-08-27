-- models/dropoff_location_dim.sql
{{
  config(
    materialized='table'
  )
}}
SELECT
  ROW_NUMBER() OVER () AS dropoff_location_id,
  dropoff_latitude,
  dropoff_longitude
FROM (
  SELECT DISTINCT dropoff_latitude, dropoff_longitude FROM {{ source('uber_data', 'raw_uberdata') }}
) AS dropoff_location
