WITH count_status AS (
    SELECT
        status_description,
        COUNT(*) AS count
    FROM {{ ref('int_production') }}
    LEFT JOIN {{ ref('int_status')}} AS status_list
        ON int_production.production_status = status_list.status_id
    GROUP BY status_description
) SELECT * FROM count_status