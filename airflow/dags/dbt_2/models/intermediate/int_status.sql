WITH int_status AS (
    SELECT 
        status_id,
        ROW_NUMBER() OVER (PARTITION BY status_id ORDER BY status_id) AS row_num,
        INITCAP(TRIM(status_description)) AS status_description
    FROM {{ref("st_status")}}
) SELECT * FROM int_status
WHERE row_num = 1

