WITH produced_by_branch AS (
    SELECT factory_branch, ROUND(SUM(production_value::numeric), 2) AS produced_by_branch
    FROM {{ref('int_machines')}} AS machines
    RIGHT JOIN {{ref('int_production')}} AS prod
        ON machines.machine_id = prod.machine_id
    GROUP BY factory_branch
) SELECT * FROM produced_by_branch