{{-
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__ANCHOR', 'true') == 'true',
        tags = ['dbt_package_ga4', 'anchor'],
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['user_id', 'ga4_user_id'],
        partition_by = {
            "field": "ga4_date_partition",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ['user_id', 'ga4_user_id']
    )
-}}


WITH t1 AS (
    SELECT
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS ga4_date_partition,
        events.user_pseudo_id AS ga4_user_id,
        TIMESTAMP_MICROS(events.event_timestamp) AS ga4_user_timestamp_updated,
        user_id
    FROM
        {{ source('dbt_package_ga4', 'events') }} AS events
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
        t1.ga4_user_id,
        MAX(t1.ga4_user_timestamp_updated) AS ga4_user_timestamp_updated,
        t1.user_id
    FROM
        t1
    WHERE
        t1.ga4_date_partition IS NOT NULL
        AND t1.ga4_user_id IS NOT NULL
        AND t1.ga4_user_timestamp_updated IS NOT NULL
        AND t1.user_id IS NOT NULL
    GROUP BY
        t1.ga4_date_partition,
        t1.ga4_user_id,
        t1.user_id
),

final AS (
    SELECT
        t2.ga4_date_partition,
        t2.ga4_user_id,
        t2.ga4_user_timestamp_updated,
        t2.user_id
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
                AND this.user_id = final.user_id
        ), TIMESTAMP('1900-01-01'))
    {% endif %}