with raw_data as (

    select
        cast(VendorID as integer) as VendorID,
        cast(tpep_pickup_datetime as timestamp) as tpep_pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as tpep_dropoff_datetime,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as float64) as trip_distance,
        cast(pickup_longitude as float64) as pickup_longitude,
        cast(pickup_latitude as float64) as pickup_latitude,
        cast(RatecodeID as integer) as RatecodeID,
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(dropoff_longitude as float64) as dropoff_longitude,
        cast(dropoff_latitude as float64) as dropoff_latitude,
        cast(payment_type as integer) as payment_type,
        cast(fare_amount as float64) as fare_amount,
        cast(extra as float64) as extra,
        cast(mta_tax as float64) as mta_tax,
        cast(tip_amount as float64) as tip_amount,
        cast(tolls_amount as float64) as tolls_amount,
        cast(improvement_surcharge as float64) as improvement_surcharge,
        cast(total_amount as float64) as total_amount

    from {{ source('uber_data', 'raw_uberdata') }}

)

select * from raw_data;
