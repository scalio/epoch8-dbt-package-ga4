{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_user_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = 'ga4_user_id'
    )
}}


WITH t1 AS (
    SELECT
        events.user_pseudo_id AS ga4_user_id,
        DATE(TIMESTAMP_MICROS(events.user_first_touch_timestamp)) AS ga4_user_date,
        events.privacy_info.uses_transient_token AS ga4_user_privacy_info_uses_transient_token
    FROM
        {{ source('ga4', 'events') }} AS events
    WHERE
        _TABLE_SUFFIX NOT LIKE '%intraday%'
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR_INTERVAL') }} DAY)
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

t2 AS (
    SELECT DISTINCT
        t1.ga4_user_id,
        t1.ga4_user_date,
        t1.ga4_user_privacy_info_uses_transient_token
    FROM
        t1
    WHERE
        t1.ga4_user_id IS NOT NULL
        AND t1.ga4_user_date IS NOT NULL
        AND t1.ga4_user_privacy_info_uses_transient_token IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_user_id,
        t2.ga4_user_date,
        t2.ga4_user_privacy_info_uses_transient_token
    FROM
        t2
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
