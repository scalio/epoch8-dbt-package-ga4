{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_event_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ['ga4_event_id', 'ga4_event_params_key']
    )
}}


WITH t1 AS (
    SELECT
        ga4_event_params.ga4_event_id,
        ga4_event_params.ga4_event_timestamp,
        ga4_event_params.ga4_event_params_key,
        ga4_event_params.ga4_event_params_string_value,
        ga4_event_params.ga4_event_params_int_value,
        ga4_event_params.ga4_event_params_double_value,
        ga4_event_params.ga4_event_params_float_value
    FROM
        {{ ref('attr__ga4_event__ga4_event_params') }} AS ga4_event_params
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
    WHERE
        ga4_event_params.ga4_date_partition > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        AND DATE(ga4_event_params.ga4_event_timestamp) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_event_id,
        t1.ga4_event_timestamp,
        t1.ga4_event_params_key,
        t1.ga4_event_params_string_value,
        t1.ga4_event_params_int_value,
        t1.ga4_event_params_double_value,
        t1.ga4_event_params_float_value
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        DATE(final.ga4_event_timestamp) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
