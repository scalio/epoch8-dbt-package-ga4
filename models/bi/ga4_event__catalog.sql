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
        ga4_event_name.ga4_event_name,
        ga4_event_name.ga4_event_appearance_timestamp
    FROM
        {{ ref('anchor__ga4_event__catalog') }} AS ga4_event_name
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
    WHERE
        ga4_event_name.ga4_date_partition >= DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_event_name,
        t1.ga4_event_appearance_timestamp
    FROM
        t1
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
