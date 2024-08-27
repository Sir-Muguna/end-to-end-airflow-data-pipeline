-- models/payment_type_dim.sql
{{
  config(
    materialized='table'
  )
}}
SELECT
  ROW_NUMBER() OVER () AS payment_type_id,
  payment_type
FROM (
  SELECT DISTINCT payment_type FROM {{ source('uber_data', 'raw_uberdata') }}
) AS unique_payment_type
