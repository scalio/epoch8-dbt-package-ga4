{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ga4_event_name',
        cluster_by = 'ga4_event_name'
    )
}}


WITH t1 AS (
    SELECT DISTINCT
        SAFE_CAST(events.event_name AS STRING) AS ga4_event_name
    FROM
        {{ source('ga4', 'events') }} AS events
    WHERE
        _TABLE_SUFFIX NOT LIKE '%intraday%'
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR_INTERVAL') }} DAY)
    
    {% if is_incremental() %}
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

t2 AS (
    SELECT
        t1.ga4_event_name
    FROM
        t1
    WHERE
        t1.ga4_event_name IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_event_name
    FROM
        t2
)

SELECT * FROM final
