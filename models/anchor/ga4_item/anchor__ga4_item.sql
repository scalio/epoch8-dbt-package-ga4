{{
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__ANCHOR', 'true') == 'true',
        tags = ['dbt_package_ga4', 'anchor'],
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ga4_item_id',
        partition_by = {
            "field": "ga4_date_partition",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = 'ga4_item_id'
    )
}}


WITH t1 AS (
    SELECT
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS ga4_date_partition,
        TIMESTAMP(DATETIME(TIMESTAMP_MICROS(events.event_timestamp)), '{{ env_var('DBT_PACKAGE_GA4__TIME_ZONE', '+00') }}') AS ga4_item_timestamp_updated,
        item.item_id AS ga4_item_id,
        item.item_name AS ga4_item_name,
        item.item_brand AS ga4_item_brand,
        item.item_variant AS ga4_item_variant,
        item.item_category AS ga4_item_category,
        item.item_category2 AS ga4_item_category2,
        item.item_category3 AS ga4_item_category3,
        item.item_category4 AS ga4_item_category4,
        item.item_category5 AS ga4_item_category5,
        item.price_in_usd AS ga4_item_price_in_usd,
        item.price AS ga4_item_price,
        item.quantity AS ga4_item_quantity,
        item.item_revenue_in_usd AS ga4_item_revenue_in_usd,
        item.item_revenue AS ga4_item_revenue,
        item.item_refund_in_usd AS ga4_item_refund_in_usd,
        item.item_refund AS ga4_item_refund,
        item.coupon AS ga4_item_coupon,
        item.affiliation AS ga4_item_affiliation,
        item.location_id AS ga4_item_location_id,
        item.item_list_id AS ga4_item_list_id,
        item.item_list_name AS ga4_item_list_name,
        item.item_list_index AS ga4_item_list_index,
        item.promotion_id AS ga4_item_promotion_id,
        item.promotion_name AS ga4_item_promotion_name,
        item.creative_name AS ga4_item_creative_name,
        item.creative_slot AS ga4_item_creative_slot
    FROM
        {{ source('dbt_package_ga4', 'events') }} AS events,
        UNNEST(events.items) AS item
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
        t1.ga4_item_creative_slot,
        ROW_NUMBER() OVER(PARTITION BY t1.ga4_item_id ORDER BY t1.ga4_item_timestamp_updated DESC) AS rn
    FROM
        t1
    WHERE
        t1.ga4_date_partition IS NOT NULL
        AND t1.ga4_item_timestamp_updated IS NOT NULL
        AND t1.ga4_item_id IS NOT NULL
),

t3 AS (
    SELECT
        t2.ga4_date_partition,
        t2.ga4_item_timestamp_updated,
        t2.ga4_item_id,
        t2.ga4_item_name,
        t2.ga4_item_brand,
        t2.ga4_item_variant,
        t2.ga4_item_category,
        t2.ga4_item_category2,
        t2.ga4_item_category3,
        t2.ga4_item_category4,
        t2.ga4_item_category5,
        t2.ga4_item_price_in_usd,
        t2.ga4_item_price,
        t2.ga4_item_quantity,
        t2.ga4_item_revenue_in_usd,
        t2.ga4_item_revenue,
        t2.ga4_item_refund_in_usd,
        t2.ga4_item_refund,
        t2.ga4_item_coupon,
        t2.ga4_item_affiliation,
        t2.ga4_item_location_id,
        t2.ga4_item_list_id,
        t2.ga4_item_list_name,
        t2.ga4_item_list_index,
        t2.ga4_item_promotion_id,
        t2.ga4_item_promotion_name,
        t2.ga4_item_creative_name,
        t2.ga4_item_creative_slot
    FROM
        t2
    WHERE
        t2.rn = 1
),


final AS (
    SELECT
        t3.ga4_date_partition,
        t3.ga4_item_timestamp_updated,
        t3.ga4_item_id,
        t3.ga4_item_name,
        t3.ga4_item_brand,
        t3.ga4_item_variant,
        t3.ga4_item_category,
        t3.ga4_item_category2,
        t3.ga4_item_category3,
        t3.ga4_item_category4,
        t3.ga4_item_category5,
        t3.ga4_item_price_in_usd,
        t3.ga4_item_price,
        t3.ga4_item_quantity,
        t3.ga4_item_revenue_in_usd,
        t3.ga4_item_revenue,
        t3.ga4_item_refund_in_usd,
        t3.ga4_item_refund,
        t3.ga4_item_coupon,
        t3.ga4_item_affiliation,
        t3.ga4_item_location_id,
        t3.ga4_item_list_id,
        t3.ga4_item_list_name,
        t3.ga4_item_list_index,
        t3.ga4_item_promotion_id,
        t3.ga4_item_promotion_name,
        t3.ga4_item_creative_name,
        t3.ga4_item_creative_slot
    FROM
        t3
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
        ), TIMESTAMP('1900-01-01'))
    {% endif %}
