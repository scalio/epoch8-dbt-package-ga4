{{
    config(
        enabled=false,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_user_properties_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ['ga4_user_id', 'ga4_user_properties_key']
    )
}}


WITH t1 AS (
    SELECT
        events.user_pseudo_id AS ga4_user_id,
        DATE(TIMESTAMP_MICROS(events.event_timestamp)) AS ga4_user_properties_date,
        ga4_user_properties.key AS ga4_user_properties_key,
        ga4_user_properties.value.string_value AS ga4_user_properties_string_value,
        ga4_user_properties.value.int_value AS ga4_user_properties_int_value,
        ga4_user_properties.value.float_value AS ga4_user_properties_float_value,
        ga4_user_properties.value.double_value AS ga4_user_properties_double_value,
        TIMESTAMP_MICROS(ga4_user_properties.value.set_timestamp_micros) AS ga4_user_properties_set_timestamp,
        TIMESTAMP_MICROS(events.event_timestamp) AS ga4_user_properties_timestamp,
    FROM
        {{ source('dbt_package_ga4', 'events') }} AS events,
        UNNEST(events.user_properties) AS ga4_user_properties
    WHERE
        _TABLE_SUFFIX NOT LIKE '%intraday%'
        AND DATE(TIMESTAMP_MICROS(events.user_first_touch_timestamp)) < DATE(CURRENT_DATE())
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL') }} DAY)
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

t2 AS (
    SELECT DISTINCT
        t1.ga4_user_id,
        t1.ga4_user_properties_date,
        t1.ga4_user_properties_key,
        t1.ga4_user_properties_string_value,
        t1.ga4_user_properties_int_value,
        t1.ga4_user_properties_float_value,
        t1.ga4_user_properties_double_value,
        t1.ga4_user_properties_set_timestamp,
        t1.ga4_user_properties_timestamp
    FROM
        t1
    WHERE
        t1.ga4_user_id IS NOT NULL
        AND t1.ga4_user_properties_date IS NOT NULL
        AND t1.ga4_user_properties_key IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_user_id,
        t2.ga4_user_properties_date,
        t2.ga4_user_properties_key,
        t2.ga4_user_properties_string_value,
        t2.ga4_user_properties_int_value,
        t2.ga4_user_properties_float_value,
        t2.ga4_user_properties_double_value,
        t2.ga4_user_properties_set_timestamp,
        t2.ga4_user_properties_timestamp
    FROM
        t2
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_user_properties_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
