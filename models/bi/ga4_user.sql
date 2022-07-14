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
    SELECT
        ga4_user.ga4_user_id,
        ga4_user.ga4_user_date,
        ga4_user_first_touch_timestamp.ga4_user_first_touch_timestamp,
        ga4_user_properties.ga4_user_properties_key,
        ga4_user_properties.ga4_user_properties_string_value,
        ga4_user_properties.ga4_user_properties_int_value,
        ga4_user_properties.ga4_user_properties_float_value,
        ga4_user_properties.ga4_user_properties_double_value,
        ga4_user_properties.ga4_user_properties_set_timestamp_micros
    FROM
        {{ ref('anchor__ga4_user') }} AS ga4_user
        LEFT JOIN {{ ref('attr__ga4_user__ga4_user_first_touch_timestamp') }} AS ga4_user_first_touch_timestamp
            ON ga4_user_first_touch_timestamp.ga4_user_id = ga4_user.ga4_user_id
        LEFT JOIN {{ ref('attr__ga4_user__ga4_user_properties') }} AS ga4_user_properties
            ON ga4_user_properties.ga4_user_id = ga4_user.ga4_user_id AND ga4_user_properties.ga4_user_date = ga4_user.ga4_user_date
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
        WHERE
            ga4_user.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            AND ga4_user_first_touch_timestamp.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            AND ga4_user_properties.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_user_id,
        t1.ga4_user_date,
        t1.ga4_user_first_touch_timestamp,
        t1.ga4_user_properties_key,
        t1.ga4_user_properties_string_value,
        t1.ga4_user_properties_int_value,
        t1.ga4_user_properties_float_value,
        t1.ga4_user_properties_double_value,
        t1.ga4_user_properties_set_timestamp_micros
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
