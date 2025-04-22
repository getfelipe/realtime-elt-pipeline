WITH produced_by_operator AS (
    SELECT operator_name, ROUND(SUM(production_value::numeric), 2) AS produced_by_operator
    FROM {{ref('int_operators')}} AS operators 
    RIGHT JOIN {{ref('int_production')}} AS prod
        ON operators.operator_id = prod.operator_id
    GROUP BY operator_name
), limit_operator AS (
    SELECT * FROM produced_by_operator
    LIMIT 5
) SELECT * FROM limit_operator
