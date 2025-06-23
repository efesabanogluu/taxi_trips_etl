with 

source as (

    select * from {{ source('taxi_trips', 'tripss') }}

),

renamed as (

    select
        pickup_date,
        drive_time,
        total_amount,
        pickup_location_id

    from source

)

select * from renamed
