with privacy as (
    select distinct
         private,
         visibility
    from {{ ref('obt_clean_activities') }}
)

select 
    *,
    row_number() over() as visibility_pk
from privacy 