{{
    config(
        enabled=false,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_session_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}


WITH t1 AS (
    SELECT
        (SELECT value.int_value FROM UNNEST(events.user_properties) WHERE key = 'ga_session_id') AS ga4_session_id,
        SAFE_CAST(events.event_date AS DATE FORMAT 'YYYYMMDD') AS ga4_session_date,
        events.geo.continent AS ga4_session_geo_continent,
        events.geo.sub_continent AS ga4_session_geo_sub_continent,
        events.geo.country AS ga4_session_geo_country,
        events.geo.region AS ga4_session_geo_region,
        events.geo.city AS ga4_session_geo_city,
        events.geo.metro AS ga4_session_geo_metro,
        events.event_timestamp
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
    SELECT DISTINCT
        t1.ga4_session_id,
        MIN(t1.ga4_session_date) OVER(PARTITION BY t1.ga4_session_id) AS ga4_session_date,
        t1.ga4_session_geo_continent,
        t1.ga4_session_geo_sub_continent,
        t1.ga4_session_geo_country,
        t1.ga4_session_geo_region,
        t1.ga4_session_geo_city,
        t1.ga4_session_geo_metro,
        ROW_NUMBER() OVER(PARTITION BY t1.ga4_session_id ORDER BY t1.event_timestamp ASC) AS rn
    FROM
        t1
    WHERE
        t1.ga4_session_id IS NOT NULL
        AND t1.ga4_session_date IS NOT NULL
),

t3 AS (
    SELECT
        t2.ga4_session_id,
        t2.ga4_session_date,
        t2.ga4_session_geo_continent,
        t2.ga4_session_geo_sub_continent,
        t2.ga4_session_geo_country,
        t2.ga4_session_geo_region,
        t2.ga4_session_geo_city,
        t2.ga4_session_geo_metro
    FROM
        t2
    WHERE
        t2.rn = 1
),

final AS (
    SELECT
        t3.ga4_session_id,
        t3.ga4_session_date,
        t3.ga4_session_geo_continent,
        t3.ga4_session_geo_sub_continent,
        t3.ga4_session_geo_country,
        t3.ga4_session_geo_region,
        t3.ga4_session_geo_city,
        t3.ga4_session_geo_metro
    FROM
        t3
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_session_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
