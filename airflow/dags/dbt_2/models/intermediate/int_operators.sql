WITH int_operators AS (
    SELECT 
        operator_id,
        ROW_NUMBER() OVER (PARTITION BY operator_id ORDER BY operator_id DESC) AS row_num,
        INITCAP(TRIM(operator_name)) AS operator_name,
        operator_cpf,
        REGEXP_REPLACE(operator_phone, '[^0-9]', '', 'g') AS operator_phone,
        LOWER(TRIM(operator_email)) AS operator_email
    FROM {{ ref('st_operators') }}
    WHERE operator_id IS NOT NULL
)
SELECT * FROM int_operators
WHERE row_num = 1
