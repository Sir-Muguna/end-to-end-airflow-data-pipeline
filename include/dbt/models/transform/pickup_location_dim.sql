-- models/pickup_location_dim.sql
{{
  config(
    materialized='table'
  )
}}
SELECT
  ROW_NUMBER() OVER () AS pickup_location_id,
  pickup_latitude,
  pickup_longitude
FROM (
  SELECT DISTINCT pickup_latitude, pickup_longitude FROM {{ source('uber_data', 'raw_uberdata') }}
) AS pickup_location
