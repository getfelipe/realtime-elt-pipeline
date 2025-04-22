WITH machine_count AS (
    SELECT 
        machine_list.machine_name,
        COUNT(*) AS total_machines
    FROM {{ ref('int_production') }} AS int_production
    LEFT JOIN {{ ref('int_machines') }} AS machine_list
        ON int_production.machine_id = machine_list.machine_id
    WHERE maintenance = 'true'
    GROUP BY machine_list.machine_name
) SELECT * FROM machine_count