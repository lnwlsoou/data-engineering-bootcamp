with source as (

    select * from {{ source('greenery', 'events') }}

),

renamed_recasted as (

    select
        event_id as event_guid
        , session_id as session_guid
        , page_url
        , created_at
        , event_type
        , user
        , order
        , product

    from source

)

select * from renamed_recasted
