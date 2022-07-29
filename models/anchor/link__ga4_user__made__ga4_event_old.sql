{{
    config(
        enabled=false,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_user__made__ga4_event__date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ['ga4_user_id', 'ga4_event_name', 'ga4_user__made__ga4_event__timestamp']
    )
}}


WITH t1 AS (
    SELECT DISTINCT
        SAFE_CAST(events.user_pseudo_id AS STRING) AS ga4_user_id,
        SAFE_CAST(events.event_name AS STRING) AS ga4_event_name,
        DATE(TIMESTAMP_MICROS(events.event_timestamp)) AS ga4_user__made__ga4_event__date,
        TIMESTAMP_MICROS(events.event_timestamp) AS ga4_user__made__ga4_event__timestamp,
        TO_HEX(
            MD5(
                CONCAT(
                    SAFE_CAST(events.event_timestamp AS STRING),
                    SAFE_CAST(events.event_name AS STRING),
                    SAFE_CAST(events.user_pseudo_id AS STRING),
                    SAFE_CAST(events.event_server_timestamp_offset AS STRING)
                    )
                )
            ) AS ga4_event_id
    FROM
        {{ source('dbt_package_ga4', 'events') }} AS events
    WHERE
        _TABLE_SUFFIX NOT LIKE '%intraday%'
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL') }} DAY)
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

t2 AS (
    SELECT
        t1.ga4_user_id,
        t1.ga4_event_name,
        t1.ga4_user__made__ga4_event__date,
        t1.ga4_user__made__ga4_event__timestamp,
        t1.ga4_event_id
    FROM
        t1
    WHERE
        t1.ga4_user_id IS NOT NULL
        AND t1.ga4_event_name IS NOT NULL
        AND t1.ga4_user__made__ga4_event__date IS NOT NULL
        AND t1.ga4_user__made__ga4_event__timestamp IS NOT NULL
        AND t1.ga4_event_id IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_user_id,
        t2.ga4_event_name,
        t2.ga4_user__made__ga4_event__date,
        t2.ga4_user__made__ga4_event__timestamp,
        t2.ga4_event_id
    FROM
        t2
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_user__made__ga4_event__date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
