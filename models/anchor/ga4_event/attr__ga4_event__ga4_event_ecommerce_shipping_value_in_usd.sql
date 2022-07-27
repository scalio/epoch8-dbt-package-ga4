{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = 'ga4_event_id'
    )
}}


WITH t1 AS (
    SELECT DISTINCT
        TO_HEX(
            MD5(
                CONCAT(
                    SAFE_CAST(events.event_timestamp AS STRING),
                    SAFE_CAST(events.event_name AS STRING),
                    SAFE_CAST(events.user_pseudo_id AS STRING),
                    SAFE_CAST(events.event_server_timestamp_offset AS STRING)
                    )
                )
            ) AS ga4_event_id,
        SAFE_CAST(events.event_date AS DATE FORMAT 'YYYYMMDD') AS ga4_event_date,
        events.ecommerce.shipping_value_in_usd AS ga4_event_ecommerce_shipping_value_in_usd
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
    SELECT
        t1.ga4_event_id,
        t1.ga4_event_date,
        t1.ga4_event_ecommerce_shipping_value_in_usd
    FROM
        t1
    WHERE
        t1.ga4_event_id IS NOT NULL
        AND t1.ga4_event_date IS NOT NULL
        AND t1.ga4_event_ecommerce_shipping_value_in_usd IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_event_id,
        t2.ga4_event_date,
        t2.ga4_event_ecommerce_shipping_value_in_usd
    FROM
        t2
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
