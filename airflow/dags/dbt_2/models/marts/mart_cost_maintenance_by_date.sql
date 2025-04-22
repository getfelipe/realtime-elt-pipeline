WITH cost_maintenance_by_date AS (
    SELECT 
        year,
        month,
        SUM(maintenance_cost) AS total_maintenance_cost
    FROM {{ ref('int_production') }}
    WHERE maintenance = 'true'
    GROUP BY year, month
)
SELECT * FROM cost_maintenance_by_date