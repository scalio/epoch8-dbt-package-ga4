{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['ga4_user_id', 'ga4_session_id'],
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
    {% set max_partition_date = macro__get_max_partition_date(this.schema, this.table) %}
    WHERE
        ga4_user__made__ga4_session.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        AND DATE(ga4_user__made__ga4_session.ga4_user__made__ga4_session__timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
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
        final.ga4_user__made__ga4_session__timestamp < COALESCE((
            SELECT
                this.ga4_user__made__ga4_session__timestamp
            FROM
                {{ this }} AS this
            WHERE
                this.ga4_user_id = final.ga4_user_id
                AND this.ga4_session_id = final.ga4_session_id
        ), TIMESTAMP(CURRENT_DATE()))
    {% endif %}
