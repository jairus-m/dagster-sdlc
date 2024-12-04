with names as (
    select 
        id, 
        name
    from {{ ref('obt_clean_activities') }}
)

select * from names