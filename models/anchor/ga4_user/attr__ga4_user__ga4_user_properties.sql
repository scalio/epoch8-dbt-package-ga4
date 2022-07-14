{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_user_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}


WITH t1 AS (
    SELECT DISTINCT
        events.user_pseudo_id AS ga4_user_id,
        SAFE_CAST(events.event_date AS DATE FORMAT 'YYYYMMDD') AS ga4_user_date,
        ga4_user_properties.key AS ga4_user_properties_key,
        ga4_user_properties.value.string_value AS ga4_user_properties_string_value,
        ga4_user_properties.value.int_value AS ga4_user_properties_int_value,
        ga4_user_properties.value.float_value AS ga4_user_properties_float_value,
        ga4_user_properties.value.double_value AS ga4_user_properties_double_value,
        ga4_user_properties.value.set_timestamp_micros AS ga4_user_properties_set_timestamp_micros
    FROM
        {{ source('ga4', 'events') }} AS events,
        UNNEST(events.user_properties) AS ga4_user_properties
    WHERE
        _TABLE_SUFFIX NOT LIKE '%intraday%'
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR_INTERVAL') }} DAY)
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

t2 AS (
    SELECT
        t1.ga4_user_id,
        t1.ga4_user_date,
        t1.ga4_user_properties_key,
        t1.ga4_user_properties_string_value,
        t1.ga4_user_properties_int_value,
        t1.ga4_user_properties_float_value,
        t1.ga4_user_properties_double_value,
        t1.ga4_user_properties_set_timestamp_micros
    FROM
        t1
    WHERE
        t1.ga4_user_id IS NOT NULL
        AND t1.ga4_user_date IS NOT NULL
        AND t1.ga4_user_properties_key IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_user_id,
        t2.ga4_user_date,
        t2.ga4_user_properties_key,
        t2.ga4_user_properties_string_value,
        t2.ga4_user_properties_int_value,
        t2.ga4_user_properties_float_value,
        t2.ga4_user_properties_double_value,
        t2.ga4_user_properties_set_timestamp_micros
    FROM
        t2
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
