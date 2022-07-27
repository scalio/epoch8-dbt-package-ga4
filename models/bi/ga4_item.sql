{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ga4_item_id',
        partition_by = {
            "field": "ga4_item_timestamp_updated",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = 'ga4_item_id'
    )
}}


WITH t1 AS (
    SELECT
        ga4_item.ga4_item_timestamp_updated,
        ga4_item.ga4_item_id,
        ga4_item.ga4_item_name,
        ga4_item.ga4_item_brand,
        ga4_item.ga4_item_variant,
        ga4_item.ga4_item_category,
        ga4_item.ga4_item_category2,
        ga4_item.ga4_item_category3,
        ga4_item.ga4_item_category4,
        ga4_item.ga4_item_category5,
        ga4_item.ga4_item_price_in_usd,
        ga4_item.ga4_item_price,
        ga4_item.ga4_item_quantity,
        ga4_item.ga4_item_revenue_in_usd,
        ga4_item.ga4_item_revenue,
        ga4_item.ga4_item_refund_in_usd,
        ga4_item.ga4_item_refund,
        ga4_item.ga4_item_coupon,
        ga4_item.ga4_item_affiliation,
        ga4_item.ga4_item_location_id,
        ga4_item.ga4_item_list_id,
        ga4_item.ga4_item_list_name,
        ga4_item.ga4_item_list_index,
        ga4_item.ga4_item_promotion_id,
        ga4_item.ga4_item_promotion_name,
        ga4_item.ga4_item_creative_name,
        ga4_item.ga4_item_creative_slot
    FROM
        {{ ref('anchor__ga4_item') }} AS ga4_item

    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
    WHERE
        DATE(ga4_item.ga4_item_timestamp_updated) > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_item_timestamp_updated,
        t1.ga4_item_id,
        t1.ga4_item_name,
        t1.ga4_item_brand,
        t1.ga4_item_variant,
        t1.ga4_item_category,
        t1.ga4_item_category2,
        t1.ga4_item_category3,
        t1.ga4_item_category4,
        t1.ga4_item_category5,
        t1.ga4_item_price_in_usd,
        t1.ga4_item_price,
        t1.ga4_item_quantity,
        t1.ga4_item_revenue_in_usd,
        t1.ga4_item_revenue,
        t1.ga4_item_refund_in_usd,
        t1.ga4_item_refund,
        t1.ga4_item_coupon,
        t1.ga4_item_affiliation,
        t1.ga4_item_location_id,
        t1.ga4_item_list_id,
        t1.ga4_item_list_name,
        t1.ga4_item_list_index,
        t1.ga4_item_promotion_id,
        t1.ga4_item_promotion_name,
        t1.ga4_item_creative_name,
        t1.ga4_item_creative_slot
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_item_timestamp_updated > COALESCE((
            SELECT
                this.ga4_item_timestamp_updated
            FROM
                {{ this }} AS this
            WHERE
                this.ga4_item_id = final.ga4_item_id
        ), TIMESTAMP(CURRENT_DATE()))
    {% endif %}
