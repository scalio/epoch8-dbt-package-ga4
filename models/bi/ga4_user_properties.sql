{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['ga4_user_id', 'ga4_user_properties_key'],
        partition_by = {
            "field": "ga4_user_properties_timestamp_updated",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ['ga4_user_id', 'ga4_user_properties_key']
    )
}}


WITH t1 AS (
    SELECT
        ga4_user_properties.ga4_user_id,
        ga4_user_properties.ga4_user_properties_timestamp_updated,
        ga4_user_properties.ga4_user_properties_key,
        ga4_user_properties.ga4_user_properties_string_value,
        ga4_user_properties.ga4_user_properties_int_value,
        ga4_user_properties.ga4_user_properties_float_value,
        ga4_user_properties.ga4_user_properties_double_value,
        ga4_user_properties.ga4_user_properties_set_timestamp
    FROM
        {{ ref('attr__ga4_user__ga4_user_properties') }} AS ga4_user_properties
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
    WHERE
        DATE(ga4_user_properties.ga4_user_properties_timestamp_updated) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_user_id,
        t1.ga4_user_properties_timestamp_updated,
        t1.ga4_user_properties_key,
        t1.ga4_user_properties_string_value,
        t1.ga4_user_properties_int_value,
        t1.ga4_user_properties_float_value,
        t1.ga4_user_properties_double_value,
        t1.ga4_user_properties_set_timestamp
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_user_properties_timestamp_updated > COALESCE((
            SELECT
                this.ga4_user_properties_timestamp_updated
            FROM
                {{ this }} AS this
            WHERE
                this.ga4_user_id = final.ga4_user_id
                AND this.ga4_user_properties_key = final.ga4_user_properties_key
        ), TIMESTAMP('1900-01-01'))
    {% endif %}
