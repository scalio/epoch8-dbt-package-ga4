{{-
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__ANCHOR', 'true') == 'true',
        tags = ['dbt_package_ga4', 'anchor'],
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['ga4_user_id', 'ga4_session_id'],
        partition_by = {
            "field": "ga4_date_partition",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ['ga4_user_id', 'ga4_session_id']
    )
-}}


WITH t1 AS (
    SELECT DISTINCT
        PARSE_DATE('%Y%m%d', TABLE_SUFFIX) AS ga4_date_partition,
        events.user_pseudo_id AS ga4_user_id,
        TO_HEX(
            MD5(
                CONCAT(
                    SAFE_CAST(events.user_pseudo_id AS STRING),
                    COALESCE(
                        SAFE_CAST((SELECT value.int_value FROM UNNEST(events.event_params) WHERE key = 'ga_session_id') AS STRING),
                        SAFE_CAST((SELECT value.int_value FROM UNNEST(events.user_properties) WHERE key = 'ga_session_id') AS STRING)
                    )
                )
            )
        ) AS ga4_session_id,
        TIMESTAMP(DATETIME(TIMESTAMP_MICROS(events.event_timestamp), '{{ env_var('DBT_PACKAGE_GA4__TIME_ZONE', '+00') }}')) AS ga4_user__made__ga4_session__timestamp
    FROM
        {{ ref('src_ga4__events') }} AS events
    WHERE
        TABLE_SUFFIX NOT LIKE '%intraday%'
    {%- if not is_incremental() %}
        AND PARSE_DATE('%Y%m%d', TABLE_SUFFIX) > DATE_SUB(DATE(CURRENT_DATE()), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL') }} DAY)
    {%- endif %}

    {%- if is_incremental() %}
    {%- set max_partition_date = macro__get_max_partition_date(this.schema, this.table) %}
        AND PARSE_DATE('%Y%m%d', TABLE_SUFFIX) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

t2 AS (
    SELECT
        t1.ga4_date_partition,
        t1.ga4_user_id,
        t1.ga4_session_id,
        t1.ga4_user__made__ga4_session__timestamp,
        ROW_NUMBER() OVER(PARTITION BY t1.ga4_user_id, t1.ga4_session_id ORDER BY t1.ga4_user__made__ga4_session__timestamp ASC) AS rn
    FROM
        t1
    WHERE
        t1.ga4_date_partition IS NOT NULL
        AND t1.ga4_user_id IS NOT NULL
        AND t1.ga4_session_id IS NOT NULL
        AND t1.ga4_user__made__ga4_session__timestamp IS NOT NULL
),

t3 AS (
    SELECT
        t2.ga4_date_partition,
        t2.ga4_user_id,
        t2.ga4_session_id,
        t2.ga4_user__made__ga4_session__timestamp
    FROM
        t2
    WHERE
        t2.rn = 1
),

final AS (
    SELECT
        t3.ga4_date_partition,
        t3.ga4_user_id,
        t3.ga4_session_id,
        t3.ga4_user__made__ga4_session__timestamp
    FROM
        t3
)

SELECT * FROM final

    {%- if is_incremental() %}
    WHERE
        final.ga4_user__made__ga4_session__timestamp < COALESCE((
            SELECT
                this.ga4_user__made__ga4_session__timestamp
            FROM
                {{ this }} AS this
            WHERE
                this.ga4_user_id = final.ga4_user_id
                AND this.ga4_session_id = final.ga4_session_id
        ), TIMESTAMP(CURRENT_DATE()))
    {%- endif %}
