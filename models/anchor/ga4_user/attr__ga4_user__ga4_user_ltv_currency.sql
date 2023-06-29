{{-
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__ANCHOR', 'true') == 'true',
        tags = ['dbt_package_ga4', 'anchor'],
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ga4_user_id',
        partition_by = {
            "field": "ga4_date_partition",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = 'ga4_user_id'
    )
-}}


WITH t1 AS (
    SELECT
        PARSE_DATE('%Y%m%d', TABLE_SUFFIX) AS ga4_date_partition,
        events.user_pseudo_id AS ga4_user_id,
        TIMESTAMP(DATETIME(TIMESTAMP_MICROS(events.event_timestamp), '{{ env_var('DBT_PACKAGE_GA4__TIME_ZONE', '+00') }}')) AS ga4_user_timestamp_updated,
        events.user_ltv.currency AS ga4_user_ltv_currency,
        ROW_NUMBER() OVER(PARTITION BY events.user_pseudo_id ORDER BY events.event_timestamp DESC) AS rn
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
        t1.ga4_user_timestamp_updated,
        t1.ga4_user_ltv_currency
    FROM
        t1
    WHERE
        t1.ga4_date_partition IS NOT NULL
        AND t1.ga4_user_id IS NOT NULL
        AND t1.ga4_user_timestamp_updated IS NOT NULL
        AND t1.ga4_user_ltv_currency IS NOT NULL
        AND t1.rn = 1
),

final AS (
    SELECT
        t2.ga4_date_partition,
        t2.ga4_user_id,
        t2.ga4_user_timestamp_updated,
        t2.ga4_user_ltv_currency
    FROM
        t2
)

SELECT * FROM final

    {%- if is_incremental() %}
    WHERE
        final.ga4_user_timestamp_updated > COALESCE((
            SELECT
                this.ga4_user_timestamp_updated
            FROM
                {{ this }} AS this
            WHERE
                this.ga4_user_id = final.ga4_user_id
        ), TIMESTAMP('1900-01-01'))
    {%- endif %}
