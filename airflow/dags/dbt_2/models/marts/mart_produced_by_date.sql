WITH units_produced_by_date AS (
    SELECT 
        production_date,
        units_produced AS total_units_produced
    FROM {{ ref('int_production') }}
)
SELECT * FROM units_produced_by_date
