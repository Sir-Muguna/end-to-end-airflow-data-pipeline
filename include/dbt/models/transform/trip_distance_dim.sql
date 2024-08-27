-- models/trip_distance_dim.sql
{{
  config(
    materialized='table'
  )
}}
SELECT
  ROW_NUMBER() OVER () AS trip_distance_id,
  trip_distance
FROM (
  SELECT DISTINCT trip_distance FROM {{ source('uber_data', 'raw_uberdata') }}
) AS trip_distances
