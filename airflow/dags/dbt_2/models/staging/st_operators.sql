WITH st_operators AS (
    SELECT * 
    FROM {{source('postgres', 'operator_list')}}
) SELECT 
 *
FROM st_operators