WITH avg_produced_by_day AS (
    SELECT production_date, ROUND(AVG(production_value::numeric),2) AS avg_production
    FROM {{ref('int_production')}}
    GROUP BY production_date
) SELECT * FROM avg_produced_by_day