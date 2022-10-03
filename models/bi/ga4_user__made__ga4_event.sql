{{
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__BI', 'false') == 'true',
        tags = ['dbt_package_ga4', 'bi'],
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_user__made__ga4_event__timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ['ga4_user_id', 'ga4_event_id']
    )
}}


WITH t1 AS (
    SELECT
        ga4_user__made__ga4_event.ga4_user_id,
        ga4_user__made__ga4_event.ga4_event_id,
        ga4_user__made__ga4_event.ga4_user__made__ga4_event__timestamp
    FROM
        {{ ref('link__ga4_user__made__ga4_event') }} AS ga4_user__made__ga4_event
    
    {% if is_incremental() %}
    {% set max_partition_date = macro__get_max_partition_date(this.schema, this.table) %}
    WHERE
        ga4_user__made__ga4_event.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        AND DATE(ga4_user__made__ga4_event.ga4_user__made__ga4_event__timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_user_id,
        t1.ga4_event_id,
        t1.ga4_user__made__ga4_event__timestamp
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        DATE(final.ga4_user__made__ga4_event__timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
