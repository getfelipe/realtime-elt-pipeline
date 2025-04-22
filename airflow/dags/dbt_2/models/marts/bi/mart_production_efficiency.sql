WITH production_efficiency AS (
    SELECT ROUND((SUM(units_produced::numeric) / SUM(production_value::numeric)), 3) AS production_efficiency
    FROM {{ref('int_production')}}
) SELECT * FROM production_efficiency