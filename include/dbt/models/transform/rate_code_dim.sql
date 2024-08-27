-- models/rate_code_dim.sql
{{
  config(
    materialized='table'
  )
}}
SELECT
  ROW_NUMBER() OVER () AS rate_code_id,
  RatecodeID
FROM (
  SELECT DISTINCT RatecodeID FROM {{ source('uber_data', 'raw_uberdata') }}
) AS rate_code
