WITH dates AS (
    SELECT 
        strftime(strptime(date, '%m-%d-%Y'), '%Y%m%d') AS date_id,
        date,
        time,
        time_bin,
        extract('dow' FROM strptime(date, '%m-%d-%Y')) + 1 AS day_of_week_num,
        strftime(strptime(date, '%m-%d-%Y'), '%A') AS day_of_week,
        extract('week' FROM strptime(date, '%m-%d-%Y')) AS week_num,
        extract('month' FROM strptime(date, '%m-%d-%Y')) AS month_num,
        strftime(strptime(date, '%m-%d-%Y'), '%B') AS month
    FROM {{ ref('obt_clean_activities') }}
)

SELECT * FROM dates