{{
    config(
        enabled=false,
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
    SELECT DISTINCT
        events.user_pseudo_id AS ga4_user_id,
        SAFE_CAST(events.event_date AS DATE FORMAT 'YYYYMMDD') AS ga4_user_date,
        (SELECT value.int_value FROM UNNEST(events.user_properties) WHERE key = 'ga_session_id') AS ga4_user_properties_session_id,
        (SELECT value.int_value FROM UNNEST(events.user_properties) WHERE key = 'ga_session_number') AS ga4_user_properties_session_number,
        (SELECT value.int_value FROM UNNEST(events.user_properties) WHERE key = 'first_open_time') AS ga4_user_properties_first_open_time
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
        t1.ga4_user_date,
        t1.ga4_user_properties_session_id,
        t1.ga4_user_properties_session_number,
        t1.ga4_user_properties_first_open_time
    FROM
        t1
    WHERE
        t1.ga4_user_id IS NOT NULL
        AND t1.ga4_user_date IS NOT NULL
        AND COALESCE(t1.ga4_user_properties_session_id, t1.ga4_user_properties_session_number, t1.ga4_user_properties_first_open_time) IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_user_id,
        t2.ga4_user_date,
        t2.ga4_user_properties_session_id,
        t2.ga4_user_properties_session_number,
        t2.ga4_user_properties_first_open_time
    FROM
        t2
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
