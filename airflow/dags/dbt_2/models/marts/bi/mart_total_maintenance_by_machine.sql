WITH total_maintenance_by_machine As (
    SELECT 
        machine_name,
        COUNT(*) AS amount_maintenance
    FROM {{ref('int_machines')}}
    RIGHT JOIN {{ref('int_production')}}
        ON int_machines.machine_id = int_production.machine_id
    WHERE maintenance = 'true'
    GROUP BY machine_name
), limit_amount_machines AS (
    SELECT * FROM total_maintenance_by_machine
    limit 5
) SELECT * FROM limit_amount_machines
