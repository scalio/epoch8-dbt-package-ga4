{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ga4_event_name',
        partition_by = {
            "field": "ga4_event_appearance_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = 'ga4_event_name'
    )
}}


WITH t1 AS (
    SELECT
        SAFE_CAST(events.event_name AS STRING) AS ga4_event_name,
        TIMESTAMP_MICROS(MIN(events.event_timestamp)) AS ga4_event_appearance_timestamp
    FROM
        {{ source('dbt_package_ga4', 'events') }} AS events
    WHERE
        _TABLE_SUFFIX NOT LIKE '%intraday%'
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL') }} DAY)
    
    {% if is_incremental() %}
        AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}

    GROUP BY
        ga4_event_name
),

t2 AS (
    SELECT
        t1.ga4_event_name,
        t1.ga4_event_appearance_timestamp
    FROM
        t1
    WHERE
        t1.ga4_event_name IS NOT NULL
        AND t1.ga4_event_appearance_timestamp IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_event_name,
        t2.ga4_event_appearance_timestamp
    FROM
        t2
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_event_appearance_timestamp < COALESCE((
            SELECT
                this.ga4_event_appearance_timestamp
            FROM
                {{ this }} AS this
            WHERE
                this.ga4_event_name = final.ga4_event_name
        ), TIMESTAMP(CURRENT_DATE()))
    {% endif %}
