with bool as (
    select distinct
        has_heartrate
    from {{ ref('obt_clean_activities')}}
)

select
    *,
    ROW_NUMBER() OVER() as has_heartrate_pk
from bool