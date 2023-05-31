with

int_networkrail__movements_companies_joined as (

    select * from {{ ref('int_networkrail__movements_companies_joined') }}

)

, final as (

    select
        event_type
        , actual_timestamp_utc
        , event_source
        , train_id
        , variation_status
        , toc_id
        , company_name

    from int_networkrail__movements_companies_joined

)

select * from final