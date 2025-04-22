WITH total_value_produced AS (
    SELECT production_date, ROUND(SUM(production_value::numeric), 2) AS total_value_produced
    FROM {{ref('int_production')}} AS prod
    GROUP BY production_date
) SELECT * FROM total_value_produced