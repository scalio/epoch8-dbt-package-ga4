{{
    config(
        enabled=true,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = 'ga4_event_id'
    )
}}


WITH t1 AS (
    SELECT
        ga4_event.ga4_event_id,
        ga4_event.ga4_event_date,
        ga4_event_timestamp.ga4_event_timestamp,
        ga4_event_name.ga4_event_name,
        ga4_event_previous_timestamp.ga4_event_previous_timestamp,
        ga4_event_value_in_usd.ga4_event_value_in_usd,
        ga4_event_bundle_sequence_id.ga4_event_bundle_sequence_id,
        ga4_event_server_timestamp_offset.ga4_event_server_timestamp_offset,
        ga4_event_ecommerce_total_item_quantity.ga4_event_ecommerce_total_item_quantity,
        ga4_event_ecommerce_purchase_revenue_in_usd.ga4_event_ecommerce_purchase_revenue_in_usd,
        ga4_event_ecommerce_purchase_revenue.ga4_event_ecommerce_purchase_revenue,
        ga4_event_ecommerce_refund_value_in_usd.ga4_event_ecommerce_refund_value_in_usd,
        ga4_event_ecommerce_refund_value.ga4_event_ecommerce_refund_value,
        ga4_event_ecommerce_shipping_value_in_usd.ga4_event_ecommerce_shipping_value_in_usd,
        ga4_event_ecommerce_shipping_value.ga4_event_ecommerce_shipping_value,
        ga4_event_ecommerce_tax_value_in_usd.ga4_event_ecommerce_tax_value_in_usd,
        ga4_event_ecommerce_tax_value.ga4_event_ecommerce_tax_value,
        ga4_event_ecommerce_transaction_id.ga4_event_ecommerce_transaction_id,
        ga4_event_ecommerce_unique_items.ga4_event_ecommerce_unique_items
    FROM
        {{ ref('anchor__ga4_event') }} AS ga4_event
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_timestamp') }} AS ga4_event_timestamp
            ON ga4_event_timestamp.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_name') }} AS ga4_event_name
            ON ga4_event_name.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_previous_timestamp') }} AS ga4_event_previous_timestamp
            ON ga4_event_previous_timestamp.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_value_in_usd') }} AS ga4_event_value_in_usd
            ON ga4_event_value_in_usd.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_bundle_sequence_id') }} AS ga4_event_bundle_sequence_id
            ON ga4_event_bundle_sequence_id.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_server_timestamp_offset') }} AS ga4_event_server_timestamp_offset
            ON ga4_event_server_timestamp_offset.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_total_item_quantity') }} AS ga4_event_ecommerce_total_item_quantity
            ON ga4_event_ecommerce_total_item_quantity.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_purchase_revenue_in_usd') }} AS ga4_event_ecommerce_purchase_revenue_in_usd
            ON ga4_event_ecommerce_purchase_revenue_in_usd.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_purchase_revenue') }} AS ga4_event_ecommerce_purchase_revenue
            ON ga4_event_ecommerce_purchase_revenue.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_refund_value_in_usd') }} AS ga4_event_ecommerce_refund_value_in_usd
            ON ga4_event_ecommerce_refund_value_in_usd.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_refund_value') }} AS ga4_event_ecommerce_refund_value
            ON ga4_event_ecommerce_refund_value.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_shipping_value_in_usd') }} AS ga4_event_ecommerce_shipping_value_in_usd
            ON ga4_event_ecommerce_shipping_value_in_usd.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_shipping_value') }} AS ga4_event_ecommerce_shipping_value
            ON ga4_event_ecommerce_shipping_value.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_tax_value_in_usd') }} AS ga4_event_ecommerce_tax_value_in_usd
            ON ga4_event_ecommerce_tax_value_in_usd.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_tax_value') }} AS ga4_event_ecommerce_tax_value
            ON ga4_event_ecommerce_tax_value.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_transaction_id') }} AS ga4_event_ecommerce_transaction_id
            ON ga4_event_ecommerce_transaction_id.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_unique_items') }} AS ga4_event_ecommerce_unique_items
            ON ga4_event_ecommerce_unique_items.ga4_event_id = ga4_event.ga4_event_id
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
    WHERE
        ga4_event.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_timestamp.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_name.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_previous_timestamp.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_value_in_usd.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_bundle_sequence_id.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_server_timestamp_offset.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_total_item_quantity.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_purchase_revenue_in_usd.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_purchase_revenue.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_refund_value_in_usd.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_refund_value.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_shipping_value_in_usd.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_shipping_value.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_tax_value_in_usd.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_tax_value.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_transaction_id.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
        AND ga4_event_ecommerce_unique_items.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_event_id,
        t1.ga4_event_date,
        t1.ga4_event_timestamp,
        t1.ga4_event_name,
        t1.ga4_event_previous_timestamp,
        t1.ga4_event_value_in_usd,
        t1.ga4_event_bundle_sequence_id,
        t1.ga4_event_server_timestamp_offset,
        t1.ga4_event_ecommerce_total_item_quantity,
        t1.ga4_event_ecommerce_purchase_revenue_in_usd,
        t1.ga4_event_ecommerce_purchase_revenue,
        t1.ga4_event_ecommerce_refund_value_in_usd,
        t1.ga4_event_ecommerce_refund_value,
        t1.ga4_event_ecommerce_shipping_value_in_usd,
        t1.ga4_event_ecommerce_shipping_value,
        t1.ga4_event_ecommerce_tax_value_in_usd,
        t1.ga4_event_ecommerce_tax_value,
        t1.ga4_event_ecommerce_transaction_id,
        t1.ga4_event_ecommerce_unique_items
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_event_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR__DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
