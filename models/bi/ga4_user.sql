{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_user_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = 'ga4_user_id'
    )
}}


WITH t1 AS (
    SELECT
        ga4_user.ga4_user_id,
        ga4_user.ga4_user_date,
        ga4_user_first_touch_timestamp.ga4_user_first_touch_timestamp,
        ga4_user_ltv_revenue.ga4_user_ltv_revenue,
        ga4_user_ltv_currency.ga4_user_ltv_currency,
        ga4_user_privacy_info_ads_storage.ga4_user_privacy_info_ads_storage,
        ga4_user_privacy_info_analytics_storage.ga4_user_privacy_info_analytics_storage,
        ga4_user_privacy_info_uses_transient_token.ga4_user_privacy_info_uses_transient_token
    FROM
        {{ ref('anchor__ga4_user') }} AS ga4_user
        LEFT JOIN {{ ref('attr__ga4_user__ga4_user_first_touch_timestamp') }} AS ga4_user_first_touch_timestamp
            ON ga4_user_first_touch_timestamp.ga4_user_id = ga4_user.ga4_user_id
        LEFT JOIN {{ ref('attr__ga4_user__ga4_user_ltv_revenue') }} AS ga4_user_ltv_revenue
            ON ga4_user_ltv_revenue.ga4_user_id = ga4_user.ga4_user_id
        LEFT JOIN {{ ref('attr__ga4_user__ga4_user_ltv_currency') }} AS ga4_user_ltv_currency
            ON ga4_user_ltv_currency.ga4_user_id = ga4_user.ga4_user_id
        LEFT JOIN {{ ref('attr__ga4_user__ga4_user_privacy_info_ads_storage') }} AS ga4_user_privacy_info_ads_storage
            ON ga4_user_privacy_info_ads_storage.ga4_user_id = ga4_user.ga4_user_id
        LEFT JOIN {{ ref('attr__ga4_user__ga4_user_privacy_info_analytics_storage') }} AS ga4_user_privacy_info_analytics_storage
            ON ga4_user_privacy_info_analytics_storage.ga4_user_id = ga4_user.ga4_user_id
        LEFT JOIN {{ ref('attr__ga4_user__ga4_user_privacy_info_uses_transient_token') }} AS ga4_user_privacy_info_uses_transient_token
            ON ga4_user_privacy_info_uses_transient_token.ga4_user_id = ga4_user.ga4_user_id
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
        WHERE
            ga4_user.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            AND ga4_user_first_touch_timestamp.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            AND ga4_user_ltv_revenue.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            AND ga4_user_ltv_currency.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            AND ga4_user_privacy_info_ads_storage.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            AND ga4_user_privacy_info_analytics_storage.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            AND ga4_user_privacy_info_uses_transient_token.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_user_id,
        t1.ga4_user_date,
        t1.ga4_user_first_touch_timestamp,
        t1.ga4_user_ltv_revenue,
        t1.ga4_user_ltv_currency,
        t1.ga4_user_privacy_info_ads_storage,
        t1.ga4_user_privacy_info_analytics_storage,
        t1.ga4_user_privacy_info_uses_transient_token
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
