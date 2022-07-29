{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ga4_user_id',
        partition_by = {
            "field": "ga4_user_timestamp_updated",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = 'ga4_user_id'
    )
}}


WITH t1 AS (
    SELECT
        ga4_user.ga4_user_id,
        ga4_user.ga4_user_timestamp_updated,
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
            ga4_user.ga4_date_partition > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
            OR ga4_user_first_touch_timestamp.ga4_date_partition > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
            OR ga4_user_ltv_revenue.ga4_date_partition > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
            OR ga4_user_ltv_currency.ga4_date_partition > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
            OR ga4_user_privacy_info_ads_storage.ga4_date_partition > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
            OR ga4_user_privacy_info_analytics_storage.ga4_date_partition > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
            OR ga4_user_privacy_info_uses_transient_token.ga4_date_partition > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_user_id,
        t1.ga4_user_timestamp_updated,
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
        final.ga4_user_timestamp_updated > COALESCE((
            SELECT
                this.ga4_user_timestamp_updated
            FROM
                {{ this }} AS this
            WHERE
                this.ga4_user_id = final.ga4_user_id
        ), TIMESTAMP('1900-01-01'))
    {% endif %}
