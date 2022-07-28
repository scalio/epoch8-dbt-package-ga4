{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ga4_user_id',
        partition_by = {
            "field": "ga4_user_timestamp_updated",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = 'ga4_user_id'
    )
}}


WITH t1 AS (
    SELECT
        events.user_pseudo_id AS ga4_user_id,
        TIMESTAMP_MICROS(events.event_timestamp) AS ga4_user_timestamp_updated,
        TIMESTAMP_MICROS(events.user_first_touch_timestamp) AS ga4_user_first_touch_timestamp,
        ROW_NUMBER() OVER(PARTITION BY events.user_pseudo_id ORDER BY events.event_timestamp DESC) AS rn
    FROM
        {{ source('dbt_package_ga4', 'events') }} AS events
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
    SELECT
        t1.ga4_user_id,
        t1.ga4_user_timestamp_updated,
        t1.ga4_user_first_touch_timestamp
    FROM
        t1
    WHERE
        t1.ga4_user_id IS NOT NULL
        AND t1.ga4_user_timestamp_updated IS NOT NULL
        AND t1.ga4_user_first_touch_timestamp IS NOT NULL
        AND t1.rn = 1
),

final AS (
    SELECT
        t2.ga4_user_id,
        t2.ga4_user_timestamp_updated,
        t2.ga4_user_first_touch_timestamp
    FROM
        t2
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_user_timestamp_updated > COALESCE((
            SELECT
                this.ga4_user_timestamp_updated
            FROM
                {{ this }} AS this
            WHERE
                this.ga4_user_id = final.ga4_user_id
        ), TIMESTAMP('1900-01-01'))
    {% endif %}
