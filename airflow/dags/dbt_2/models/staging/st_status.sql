WITH st_status AS (
    SELECT * 
    FROM {{source('postgres', 'status_table')}}
) SELECT 
  *
FROM st_status