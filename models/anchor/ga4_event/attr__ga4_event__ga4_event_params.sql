{{
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__BI', 'true') == 'true',
        tags = ['dbt_package_ga4', 'anchor'],
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_date_partition",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ['ga4_event_id', 'ga4_event_params_key']
    )
}}


WITH t1 AS (
    SELECT DISTINCT
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS ga4_date_partition,
        TO_HEX(
            MD5(
                CONCAT(
                    SAFE_CAST(events.event_timestamp AS STRING),
                    SAFE_CAST(events.event_name AS STRING),
                    SAFE_CAST(events.user_pseudo_id AS STRING)
                    )
                )
            ) AS ga4_event_id,
        TIMESTAMP_MICROS(events.event_timestamp) AS ga4_event_timestamp,
        ga4_event_params.key AS ga4_event_params_key,
        ga4_event_params.value.string_value AS ga4_event_params_string_value,
        ga4_event_params.value.int_value AS ga4_event_params_int_value,
        ga4_event_params.value.float_value AS ga4_event_params_float_value,
        ga4_event_params.value.double_value AS ga4_event_params_double_value
    FROM
        {{ source('dbt_package_ga4', 'events') }} AS events,
        UNNEST(events.event_params) AS ga4_event_params
    WHERE
        _TABLE_SUFFIX NOT LIKE '%intraday%'
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL') }} DAY)
        AND events.stream_id IN UNNEST({{ env_var('DBT_PACKAGE_GA4__STREAM_ID') }})
    
    {% if is_incremental() %}
    {% set max_partition_date = macro__get_max_partition_date(this.schema, this.table) %}
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

t2 AS (
    SELECT
        t1.ga4_date_partition,
        t1.ga4_event_id,
        t1.ga4_event_timestamp,
        t1.ga4_event_params_key,
        t1.ga4_event_params_string_value,
        t1.ga4_event_params_int_value,
        t1.ga4_event_params_float_value,
        t1.ga4_event_params_double_value
    FROM
        t1
    WHERE
        t1.ga4_date_partition IS NOT NULL
        AND t1.ga4_event_id IS NOT NULL
        AND t1.ga4_event_timestamp IS NOT NULL
        AND t1.ga4_event_params_key IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_date_partition,
        t2.ga4_event_id,
        t2.ga4_event_timestamp,
        t2.ga4_event_params_key,
        t2.ga4_event_params_string_value,
        t2.ga4_event_params_int_value,
        t2.ga4_event_params_float_value,
        t2.ga4_event_params_double_value
    FROM
        t2
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
