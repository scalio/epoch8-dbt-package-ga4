{{-
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__ANCHOR', 'true') == 'true',
        tags = ['dbt_package_ga4', 'anchor'],
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_date_partition",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ['ga4_event_id', 'ga4_item_id']
    )
-}}


WITH t1 AS (
    SELECT DISTINCT
        PARSE_DATE('%Y%m%d', TABLE_SUFFIX) AS ga4_date_partition,
        TO_HEX(
            MD5(
                CONCAT(
                    SAFE_CAST(events.event_timestamp AS STRING),
                    SAFE_CAST(events.event_name AS STRING),
                    SAFE_CAST(events.user_pseudo_id AS STRING)
                    )
                )
            ) AS ga4_event_id,
        item.item_id AS ga4_item_id,
        TIMESTAMP(DATETIME(TIMESTAMP_MICROS(events.event_timestamp), '{{ env_var('DBT_PACKAGE_GA4__TIME_ZONE', '+00') }}')) AS ga4_event__contains__ga4_item__timestamp
    FROM
        {{ ref('src_ga4__events') }} AS events,
        UNNEST(events.items) AS item
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
        t1.ga4_event_id,
        t1.ga4_item_id,
        t1.ga4_event__contains__ga4_item__timestamp
    FROM
        t1
    WHERE
        t1.ga4_date_partition IS NOT NULL
        AND t1.ga4_event_id IS NOT NULL
        AND t1.ga4_item_id IS NOT NULL
        AND t1.ga4_event__contains__ga4_item__timestamp IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_date_partition,
        t2.ga4_event_id,
        t2.ga4_item_id,
        t2.ga4_event__contains__ga4_item__timestamp
    FROM
        t2
)

SELECT * FROM final

    {%- if is_incremental() %}
    WHERE
        final.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
