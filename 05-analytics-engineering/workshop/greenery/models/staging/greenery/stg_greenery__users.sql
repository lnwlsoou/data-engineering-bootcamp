-- select * from `effective-sonar-384416.deb_workshop.users` old sql is hard code, cannot reuse
-- select 
--     * 
-- from {{ source('greenery', 'users') }}

----------------------------------
-- old pattern Nested: this bad. because, If you select table in table in table, it too complex. it cannot optimize
    -- select
    --     user_id as user_guid
    --     , first_name
    --     , last_name
    --     , email
    --     , phone_number
    --     , created_at as created_at_utc
    --     , updated_at as updated_at_utc
    --     , address as address_guid

    -- from (
    --     select * from {{ source('greenery', 'users') }}
    -- )



-------------------------
-- new pattern CTE, write code to 3 part

-- part1 : select source to table source
with source as (

    select * from {{ source('greenery', 'users') }}

)
-- part2 : transformation logic
, renamed_recasted as (

    select
        user_id as user_guid
        , first_name
        , last_name
        , email
        , phone_number
        , created_at as created_at_utc
        , updated_at as updated_at_utc
        , address as address_guid

    from source

)
-- part3: export
select * from renamed_recasted