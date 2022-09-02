{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_timeline_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ['ga4_user_id', 'ga4_session_id', 'ga4_event_id']
    )
}}


WITH ga4_event_name AS (
    SELECT
        ga4_event_name.ga4_event_id,
        ga4_event_name.ga4_event_name
    FROM
        {{ ref('attr__ga4_event__ga4_event_name') }} AS ga4_event_name

    {% if is_incremental() %}
    {% set max_partition_date = macro__get_max_partition_date(this.schema, this.table) %}
    WHERE
        ga4_event_name.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_session__contains__ga4_event AS (
    SELECT
        ga4_session__contains__ga4_event.ga4_session_id,
        ga4_session__contains__ga4_event.ga4_event_id
    FROM
        {{ ref('link__ga4_session__contains__ga4_event') }} AS ga4_session__contains__ga4_event
    
    {% if is_incremental() %}
    WHERE
        ga4_session__contains__ga4_event.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        AND DATE(ga4_session__contains__ga4_event.ga4_session__contains__ga4_event__timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

ga4_event__contains__ga4_item AS (
    SELECT
        ga4_event__contains__ga4_item.ga4_event_id,
        ga4_event__contains__ga4_item.ga4_item_id
    FROM
        {{ ref('link__ga4_event__contains__ga4_item') }} AS ga4_event__contains__ga4_item
    
    {% if is_incremental() %}
    WHERE
        ga4_event__contains__ga4_item.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        AND DATE(ga4_event__contains__ga4_item.ga4_event__contains__ga4_item__timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

t1 AS (
    SELECT
        ga4_user__made__ga4_event.ga4_user__made__ga4_event__timestamp AS ga4_timeline_timestamp,
        ga4_user__made__ga4_event.ga4_user_id,
        ga4_session__contains__ga4_event.ga4_session_id,
        ga4_user__made__ga4_event.ga4_event_id,
        ga4_event_name.ga4_event_name,
        ga4_event__contains__ga4_item.ga4_item_id,
        IF(ga4_event__contains__ga4_item.ga4_item_id IS NULL, NULL, ROW_NUMBER() OVER(
            PARTITION BY
                ga4_user__made__ga4_event.ga4_user__made__ga4_event__timestamp,
                ga4_user__made__ga4_event.ga4_user_id,
                ga4_session__contains__ga4_event.ga4_session_id,
                ga4_user__made__ga4_event.ga4_event_id,
                ga4_event_name.ga4_event_name
            ORDER BY
                ga4_event__contains__ga4_item.ga4_item_id ASC
        )) AS n_item
    FROM
        {{ ref('link__ga4_user__made__ga4_event') }} AS ga4_user__made__ga4_event
        LEFT JOIN ga4_event_name
            ON ga4_event_name.ga4_event_id = ga4_user__made__ga4_event.ga4_event_id
        LEFT JOIN ga4_session__contains__ga4_event
            ON ga4_session__contains__ga4_event.ga4_event_id = ga4_user__made__ga4_event.ga4_event_id
        LEFT JOIN ga4_event__contains__ga4_item
            ON ga4_event__contains__ga4_item.ga4_event_id = ga4_user__made__ga4_event.ga4_event_id
    
    {% if is_incremental() %}
    WHERE
        ga4_user__made__ga4_event.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        AND DATE(ga4_user__made__ga4_event.ga4_user__made__ga4_event__timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_timeline_timestamp,
        t1.ga4_user_id,
        t1.ga4_session_id,
        t1.ga4_event_id,
        t1.ga4_event_name,
        t1.ga4_item_id,
        t1.n_item
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        DATE(final.ga4_timeline_timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
