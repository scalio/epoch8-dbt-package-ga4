{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_date_partition",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = 'ga4_event_id'
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
        TIMESTAMP_MICROS(events.event_server_timestamp_offset) AS ga4_event_server_timestamp_offset
    FROM
        {{ source('dbt_package_ga4', 'events') }} AS events
    WHERE
        _TABLE_SUFFIX NOT LIKE '%intraday%'
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL') }} DAY)
        AND events.stream_id IN UNNEST({{ var('VAR__DBT_PACKAGE_GA4__STREAM_ID') }})
    
    {% if is_incremental() %}
    {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

t2 AS (
    SELECT
        t1.ga4_date_partition,
        t1.ga4_event_id,
        t1.ga4_event_server_timestamp_offset,
        ROW_NUMBER() OVER(PARTITION BY t1.ga4_event_id ORDER BY t1.ga4_event_server_timestamp_offset DESC) AS rn
    FROM
        t1
    WHERE
        t1.ga4_date_partition IS NOT NULL
        AND t1.ga4_event_id IS NOT NULL
        AND t1.ga4_event_server_timestamp_offset IS NOT NULL
),

t3 AS (
    SELECT
        t2.ga4_date_partition,
        t2.ga4_event_id,
        t2.ga4_event_server_timestamp_offset
    FROM
        t2
    WHERE
        t2.rn = 1
),

final AS (
    SELECT
        t3.ga4_date_partition,
        t3.ga4_event_id,
        t3.ga4_event_server_timestamp_offset
    FROM
        t3
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_date_partition > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
