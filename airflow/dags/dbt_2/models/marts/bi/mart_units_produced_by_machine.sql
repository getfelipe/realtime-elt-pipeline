WITH units_produced_by_machine AS (
    SELECT
        machine_name,
        SUM(units_produced) AS units_produced_by_machine
    FROM {{ref('int_machines')}}
    RIGHT JOIN {{ref('int_production')}}
        ON int_machines.machine_id = int_production.machine_id
    WHERE maintenance = 'true'
    GROUP BY machine_name
) SELECT * FROM units_produced_by_machine