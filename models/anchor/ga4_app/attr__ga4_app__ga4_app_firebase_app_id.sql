{{
    config(
        enabled=false,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['ga4_app_id', 'ga4_app_firebase_app_id'],
        partition_by = {
            "field": "ga4_app_firebase_appearance_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = 'ga4_app_id'
    )
}}


WITH t1 AS (
    SELECT
        SAFE_CAST(events.app_info.id AS STRING) AS ga4_app_id,
        events.app_info.firebase_app_id AS ga4_app_firebase_app_id,
        TIMESTAMP_MICROS(MIN(events.event_timestamp)) AS ga4_app_firebase_appearance_timestamp
    FROM
        {{ source('ga4', 'events') }} AS events
    WHERE
        _TABLE_SUFFIX NOT LIKE '%intraday%'
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR_INTERVAL') }} DAY)
    
    {% if is_incremental() %}
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}

    GROUP BY
        ga4_app_id,
        ga4_app_firebase_app_id
),

t2 AS (
    SELECT
        t1.ga4_app_id,
        t1.ga4_app_firebase_app_id,
        t1.ga4_app_firebase_appearance_timestamp
    FROM
        t1
    WHERE
        t1.ga4_app_id IS NOT NULL
        AND t1.ga4_app_firebase_app_id IS NOT NULL
        AND t1.ga4_app_firebase_appearance_timestamp IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_app_id,
        t2.ga4_app_firebase_app_id,
        t2.ga4_app_firebase_appearance_timestamp
    FROM
        t2
)

SELECT * FROM final
