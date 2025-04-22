WITH st_machine AS (
    SELECT * 
    FROM {{source('postgres', 'machine_list')}}
) SELECT 
 * FROM st_machine