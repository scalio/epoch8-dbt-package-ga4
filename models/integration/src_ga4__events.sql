{{-
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__INTEGRATION', 'true') == 'true',
        tags = ['dbt_package_ga4', 'integration'],
        materialized = 'view'
    )
-}}


WITH raw AS (
    SELECT
        events._TABLE_SUFFIX AS TABLE_SUFFIX,
        events.*
    FROM
        {{ source('dbt_package_ga4', 'events') }} AS events
    WHERE
        events.stream_id IN UNNEST({{ env_var('DBT_PACKAGE_GA4__STREAM_ID') }})
)

SELECT * FROM raw
