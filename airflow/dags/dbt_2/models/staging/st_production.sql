WITH raw_prod AS (
    SELECT * 
    FROM {{source('postgres', 'production_list')}}
) SELECT 
 *
FROM raw_prod