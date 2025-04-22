WITH int_machines AS (
    SELECT 
        machine_id,
        ROW_NUMBER() OVER (PARTITION BY machine_id ORDER BY machine_id) AS row_num,
        INITCAP(TRIM(machine_name)) AS machine_name,
        INITCAP(TRIM(factory_branch)) AS factory_branch
    FROM {{ref('st_machines')}}
) SELECT 
 *
FROM int_machines
WHERE row_num = 1