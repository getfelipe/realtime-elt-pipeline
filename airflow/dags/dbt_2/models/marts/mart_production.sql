WITH mart_production AS (
    SELECT 
           prod.production_date,
           prod.production_value,
           prod.units_produced,
           prod.maintenance,
           prod.maintenance_time,
           prod.maintenance_cost,
           prod.operator_id,
           operators.operator_name, 
           machine_list.machine_name, 
           status_list.status_description
    FROM {{ ref('int_production') }} AS prod

    LEFT JOIN {{ ref('int_operators') }} AS operators
        ON prod.operator_id = operators.operator_id

    LEFT JOIN {{ref('int_machines')}} AS machine_list
    ON prod.machine_id = machine_list.machine_id

    LEFT JOIN {{ref('int_status')}} AS status_list
    ON prod.production_status = status_list.status_id
)
SELECT * FROM mart_production
