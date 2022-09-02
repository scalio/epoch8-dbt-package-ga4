{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_event__contains__ga4_item__timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ['ga4_event_id', 'ga4_item_id']
    )
}}


WITH t1 AS (
    SELECT
        ga4_event__contains__ga4_item.ga4_event_id,
        ga4_event__contains__ga4_item.ga4_item_id,
        ga4_event__contains__ga4_item.ga4_event__contains__ga4_item__timestamp
    FROM
        {{ ref('link__ga4_event__contains__ga4_item') }} AS ga4_event__contains__ga4_item
    
    {% if is_incremental() %}
    {% set max_partition_date = macro__get_max_partition_date(this.schema, this.table) %}
    WHERE
        ga4_event__contains__ga4_item.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        AND DATE(ga4_event__contains__ga4_item.ga4_event__contains__ga4_item__timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_event_id,
        t1.ga4_item_id,
        t1.ga4_event__contains__ga4_item__timestamp
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        DATE(final.ga4_event__contains__ga4_item__timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
