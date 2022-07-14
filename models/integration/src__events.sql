{{
    config(
        enabled=false
    )
}}


WITH raw AS (
    SELECT
        *
    FROM
        {{ source('ga4', 'events') }}
)

SELECT * FROM raw
