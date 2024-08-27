-- models/passenger_count_dim.sql
{{
  config(
    materialized='table'
  )
}}
SELECT
  ROW_NUMBER() OVER () AS passenger_count_id,
  passenger_count
FROM (
  SELECT DISTINCT passenger_count FROM {{ source('uber_data', 'raw_uberdata') }}
) AS unique_passenger_count
