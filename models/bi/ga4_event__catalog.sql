{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ga4_event_name',
        partition_by = {
            "field": "ga4_event_appearance_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = 'ga4_event_name'
    )
}}


WITH t1 AS (
    SELECT
        ga4_event_name.ga4_event_name,
        ga4_event_appearance_timestamp.ga4_event_appearance_timestamp
    FROM
        {{ ref('anchor__ga4_event__catalog') }} AS ga4_event_name
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_appearance_timestamp') }} AS ga4_event_appearance_timestamp
            ON ga4_event_appearance_timestamp.ga4_event_name = ga4_event_name.ga4_event_name
),

final AS (
    SELECT
        t1.ga4_event_name,
        t1.ga4_event_appearance_timestamp
    FROM
        t1
)

SELECT * FROM final
