{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_user__made__ga4_session__timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ['ga4_user_id', 'ga4_session_id']
    )
}}


WITH t1 AS (
    SELECT
        ga4_user__made__ga4_session.ga4_user_id,
        ga4_user__made__ga4_session.ga4_session_id,
        ga4_user__made__ga4_session.ga4_user__made__ga4_session__timestamp
    FROM
        {{ ref('link__ga4_user__made__ga4_session') }} AS ga4_user__made__ga4_session
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
        WHERE
            DATE(ga4_user__made__ga4_session.ga4_user__made__ga4_session__timestamp) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_user_id,
        t1.ga4_session_id,
        t1.ga4_user__made__ga4_session__timestamp
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        DATE(final.ga4_user__made__ga4_session__timestamp) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
