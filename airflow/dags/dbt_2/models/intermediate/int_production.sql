WITH intermediate_treat_prod AS (
    SELECT 
        production_id,
        ROW_NUMBER() OVER (PARTITION BY production_id ORDER BY production_id) AS row_num,
        machine_id,
        operator_id,
        production_date,
        production_value,
        production_status,
        units_produced,
        maintenance,
        maintenance_time,
        maintenance_cost
    FROM {{ ref('st_production') }}
),
filtered_prod AS (
    SELECT *
    FROM intermediate_treat_prod
    WHERE row_num = 1
),
intermediate_prod AS (
    SELECT 
        production_id,
        machine_id,
        operator_id,
        production_date,
        production_value,
        production_status,
        units_produced,
        maintenance,
        CASE 
            WHEN maintenance = 'false' THEN 0
            ELSE maintenance_time
        END AS maintenance_time,
        CASE 
            WHEN maintenance = 'false' THEN 0
            ELSE maintenance_cost
        END AS maintenance_cost
    FROM filtered_prod
),
intermediate_prod_with_month AS (
    SELECT *,
        EXTRACT(MONTH FROM production_date) AS month,
        EXTRACT(YEAR FROM production_date) AS year
    FROM intermediate_prod
) SELECT * FROM intermediate_prod_with_month
