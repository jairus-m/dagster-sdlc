WITH activity_types AS (
    SELECT DISTINCT
         sport_type
    FROM  {{ ref('obt_clean_activities')}}
)

SELECT 
    ROW_NUMBER() OVER() AS sport_type_pk,
    sport_type,
    CASE 
        {% for activity in ['VirtualRide', 'Ride', 'MountainBikeRide'] %}
            WHEN sport_type = '{{ activity }}' THEN 'Cycling'
        {% endfor %}
        {% for activity in ['Run'] %}
            WHEN sport_type = '{{ activity }}' THEN 'Running'
        {% endfor %}
        ELSE 'Other'
    END AS activity_category
FROM activity_types
