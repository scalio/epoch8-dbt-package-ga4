{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_session_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = 'ga4_session_id'
    )
}}


WITH t1 AS (
    SELECT
        -- (SELECT value.int_value FROM UNNEST(events.user_properties) WHERE key = 'ga_session_id') AS ga4_session_id,
        TO_HEX(
            MD5(
                CONCAT(
                    SAFE_CAST(events.user_pseudo_id AS STRING),
                    SAFE_CAST((SELECT value.int_value FROM UNNEST(events.user_properties) WHERE key = 'ga_session_id') AS STRING)
                    )
                )
            ) AS ga4_session_id,
        TIMESTAMP_MICROS(events.event_timestamp) AS ga4_session_timestamp,
        events.device.mobile_brand_name AS ga4_session_device_mobile_brand_name
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
        t1.ga4_session_id,
        t1.ga4_session_timestamp,
        t1.ga4_session_device_mobile_brand_name,
        ROW_NUMBER() OVER(PARTITION BY t1.ga4_session_id ORDER BY t1.ga4_session_timestamp ASC) AS rn
    FROM
        t1
    WHERE
        t1.ga4_session_id IS NOT NULL
        AND t1.ga4_session_timestamp IS NOT NULL
        AND t1.ga4_session_device_mobile_brand_name IS NOT NULL
),

t3 AS (
    SELECT
        t2.ga4_session_id,
        t2.ga4_session_timestamp,
        t2.ga4_session_device_mobile_brand_name
    FROM
        t2
    WHERE
        t2.rn = 1
),


final AS (
    SELECT
        t3.ga4_session_id,
        t3.ga4_session_timestamp,
        t3.ga4_session_device_mobile_brand_name
    FROM
        t3
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_session_timestamp <= COALESCE((
            SELECT
                this.ga4_session_timestamp
            FROM
                {{ this }} AS this
            WHERE
                this.ga4_session_id = final.ga4_session_id
        ), TIMESTAMP(CURRENT_DATE()))
    {% endif %}
