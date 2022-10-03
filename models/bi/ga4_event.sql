{{
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__BI', 'false') == 'true',
        tags = ['dbt_package_ga4', 'bi'],
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_event_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = 'ga4_event_id'
    )
}}


WITH ga4_event_date AS (
    SELECT
        ga4_event_date.ga4_event_id,
        ga4_event_date.ga4_event_date
    FROM
        {{ ref('attr__ga4_event__ga4_event_date') }} AS ga4_event_date

    {% if is_incremental() %}
    {% set max_partition_date = macro__get_max_partition_date(this.schema, this.table) %}
    WHERE
        ga4_event_date.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_timestamp AS (
    SELECT
        ga4_event_timestamp.ga4_event_id,
        ga4_event_timestamp.ga4_event_timestamp
    FROM
        {{ ref('attr__ga4_event__ga4_event_timestamp') }} AS ga4_event_timestamp

    {% if is_incremental() %}
    WHERE
        ga4_event_timestamp.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_name AS (
    SELECT
        ga4_event_name.ga4_event_id,
        ga4_event_name.ga4_event_name
    FROM
        {{ ref('attr__ga4_event__ga4_event_name') }} AS ga4_event_name

    {% if is_incremental() %}
    WHERE
        ga4_event_name.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_previous_timestamp AS (
    SELECT
        ga4_event_previous_timestamp.ga4_event_id,
        ga4_event_previous_timestamp.ga4_event_previous_timestamp
    FROM
        {{ ref('attr__ga4_event__ga4_event_previous_timestamp') }} AS ga4_event_previous_timestamp

    {% if is_incremental() %}
    WHERE
        ga4_event_previous_timestamp.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_value_in_usd AS (
    SELECT
        ga4_event_value_in_usd.ga4_event_id,
        ga4_event_value_in_usd.ga4_event_value_in_usd
    FROM
        {{ ref('attr__ga4_event__ga4_event_value_in_usd') }} AS ga4_event_value_in_usd

    {% if is_incremental() %}
    WHERE
        ga4_event_value_in_usd.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_bundle_sequence_id AS (
    SELECT
        ga4_event_bundle_sequence_id.ga4_event_id,
        ga4_event_bundle_sequence_id.ga4_event_bundle_sequence_id
    FROM
        {{ ref('attr__ga4_event__ga4_event_bundle_sequence_id') }} AS ga4_event_bundle_sequence_id

    {% if is_incremental() %}
    WHERE
        ga4_event_bundle_sequence_id.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_server_timestamp_offset AS (
    SELECT
        ga4_event_server_timestamp_offset.ga4_event_id,
        ga4_event_server_timestamp_offset.ga4_event_server_timestamp_offset
    FROM
        {{ ref('attr__ga4_event__ga4_event_server_timestamp_offset') }} AS ga4_event_server_timestamp_offset

    {% if is_incremental() %}
    WHERE
        ga4_event_server_timestamp_offset.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_total_item_quantity AS (
    SELECT
        ga4_event_ecommerce_total_item_quantity.ga4_event_id,
        ga4_event_ecommerce_total_item_quantity.ga4_event_ecommerce_total_item_quantity
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_total_item_quantity') }} AS ga4_event_ecommerce_total_item_quantity

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_total_item_quantity.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_purchase_revenue_in_usd AS (
    SELECT
        ga4_event_ecommerce_purchase_revenue_in_usd.ga4_event_id,
        ga4_event_ecommerce_purchase_revenue_in_usd.ga4_event_ecommerce_purchase_revenue_in_usd
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_purchase_revenue_in_usd') }} AS ga4_event_ecommerce_purchase_revenue_in_usd

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_purchase_revenue_in_usd.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_purchase_revenue AS (
    SELECT
        ga4_event_ecommerce_purchase_revenue.ga4_event_id,
        ga4_event_ecommerce_purchase_revenue.ga4_event_ecommerce_purchase_revenue
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_purchase_revenue') }} AS ga4_event_ecommerce_purchase_revenue

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_purchase_revenue.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_refund_value_in_usd AS (
    SELECT
        ga4_event_ecommerce_refund_value_in_usd.ga4_event_id,
        ga4_event_ecommerce_refund_value_in_usd.ga4_event_ecommerce_refund_value_in_usd
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_refund_value_in_usd') }} AS ga4_event_ecommerce_refund_value_in_usd

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_refund_value_in_usd.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_refund_value AS (
    SELECT
        ga4_event_ecommerce_refund_value.ga4_event_id,
        ga4_event_ecommerce_refund_value.ga4_event_ecommerce_refund_value
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_refund_value') }} AS ga4_event_ecommerce_refund_value

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_refund_value.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_shipping_value_in_usd AS (
    SELECT
        ga4_event_ecommerce_shipping_value_in_usd.ga4_event_id,
        ga4_event_ecommerce_shipping_value_in_usd.ga4_event_ecommerce_shipping_value_in_usd
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_shipping_value_in_usd') }} AS ga4_event_ecommerce_shipping_value_in_usd

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_shipping_value_in_usd.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_shipping_value AS (
    SELECT
        ga4_event_ecommerce_shipping_value.ga4_event_id,
        ga4_event_ecommerce_shipping_value.ga4_event_ecommerce_shipping_value
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_shipping_value') }} AS ga4_event_ecommerce_shipping_value

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_shipping_value.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_tax_value_in_usd AS (
    SELECT
        ga4_event_ecommerce_tax_value_in_usd.ga4_event_id,
        ga4_event_ecommerce_tax_value_in_usd.ga4_event_ecommerce_tax_value_in_usd
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_tax_value_in_usd') }} AS ga4_event_ecommerce_tax_value_in_usd

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_tax_value_in_usd.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_tax_value AS (
    SELECT
        ga4_event_ecommerce_tax_value.ga4_event_id,
        ga4_event_ecommerce_tax_value.ga4_event_ecommerce_tax_value
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_tax_value') }} AS ga4_event_ecommerce_tax_value

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_tax_value.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_transaction_id AS (
    SELECT
        ga4_event_ecommerce_transaction_id.ga4_event_id,
        ga4_event_ecommerce_transaction_id.ga4_event_ecommerce_transaction_id
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_transaction_id') }} AS ga4_event_ecommerce_transaction_id

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_transaction_id.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_ecommerce_unique_items AS (
    SELECT
        ga4_event_ecommerce_unique_items.ga4_event_id,
        ga4_event_ecommerce_unique_items.ga4_event_ecommerce_unique_items
    FROM
        {{ ref('attr__ga4_event__ga4_event_ecommerce_unique_items') }} AS ga4_event_ecommerce_unique_items

    {% if is_incremental() %}
    WHERE
        ga4_event_ecommerce_unique_items.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

t1 AS (
    SELECT
        ga4_event.ga4_event_id,
        ga4_event_date.ga4_event_date,
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
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_date') }} AS ga4_event_date
        LEFT JOIN ga4_event_date
            ON ga4_event_date.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_timestamp') }} AS ga4_event_timestamp
        LEFT JOIN ga4_event_timestamp
            ON ga4_event_timestamp.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_name') }} AS ga4_event_name
        LEFT JOIN ga4_event_name
            ON ga4_event_name.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_previous_timestamp') }} AS ga4_event_previous_timestamp
        LEFT JOIN ga4_event_previous_timestamp
            ON ga4_event_previous_timestamp.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_value_in_usd') }} AS ga4_event_value_in_usd
        LEFT JOIN ga4_event_value_in_usd
            ON ga4_event_value_in_usd.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_bundle_sequence_id') }} AS ga4_event_bundle_sequence_id
        LEFT JOIN ga4_event_bundle_sequence_id
            ON ga4_event_bundle_sequence_id.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_server_timestamp_offset') }} AS ga4_event_server_timestamp_offset
        LEFT JOIN ga4_event_server_timestamp_offset
            ON ga4_event_server_timestamp_offset.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_total_item_quantity') }} AS ga4_event_ecommerce_total_item_quantity
        LEFT JOIN ga4_event_ecommerce_total_item_quantity
            ON ga4_event_ecommerce_total_item_quantity.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_purchase_revenue_in_usd') }} AS ga4_event_ecommerce_purchase_revenue_in_usd
        LEFT JOIN ga4_event_ecommerce_purchase_revenue_in_usd
            ON ga4_event_ecommerce_purchase_revenue_in_usd.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_purchase_revenue') }} AS ga4_event_ecommerce_purchase_revenue
        LEFT JOIN ga4_event_ecommerce_purchase_revenue
            ON ga4_event_ecommerce_purchase_revenue.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_refund_value_in_usd') }} AS ga4_event_ecommerce_refund_value_in_usd
        LEFT JOIN ga4_event_ecommerce_refund_value_in_usd
            ON ga4_event_ecommerce_refund_value_in_usd.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_refund_value') }} AS ga4_event_ecommerce_refund_value
        LEFT JOIN ga4_event_ecommerce_refund_value
            ON ga4_event_ecommerce_refund_value.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_shipping_value_in_usd') }} AS ga4_event_ecommerce_shipping_value_in_usd
        LEFT JOIN ga4_event_ecommerce_shipping_value_in_usd
            ON ga4_event_ecommerce_shipping_value_in_usd.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_shipping_value') }} AS ga4_event_ecommerce_shipping_value
        LEFT JOIN ga4_event_ecommerce_shipping_value
            ON ga4_event_ecommerce_shipping_value.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_tax_value_in_usd') }} AS ga4_event_ecommerce_tax_value_in_usd
        LEFT JOIN ga4_event_ecommerce_tax_value_in_usd
            ON ga4_event_ecommerce_tax_value_in_usd.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_tax_value') }} AS ga4_event_ecommerce_tax_value
        LEFT JOIN ga4_event_ecommerce_tax_value
            ON ga4_event_ecommerce_tax_value.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_transaction_id') }} AS ga4_event_ecommerce_transaction_id
        LEFT JOIN ga4_event_ecommerce_transaction_id
            ON ga4_event_ecommerce_transaction_id.ga4_event_id = ga4_event.ga4_event_id
        -- LEFT JOIN {{ ref('attr__ga4_event__ga4_event_ecommerce_unique_items') }} AS ga4_event_ecommerce_unique_items
        LEFT JOIN ga4_event_ecommerce_unique_items
            ON ga4_event_ecommerce_unique_items.ga4_event_id = ga4_event.ga4_event_id
    
    {% if is_incremental() %}
    WHERE
        ga4_event.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_date.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_timestamp.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_name.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_previous_timestamp.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_value_in_usd.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_bundle_sequence_id.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_server_timestamp_offset.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_total_item_quantity.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_purchase_revenue_in_usd.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_purchase_revenue.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_refund_value_in_usd.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_refund_value.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_shipping_value_in_usd.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_shipping_value.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_tax_value_in_usd.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_tax_value.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_transaction_id.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND ga4_event_ecommerce_unique_items.ga4_date_partition > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
        -- AND DATE(ga4_event_timestamp.ga4_event_timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
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
        DATE(final.ga4_event_timestamp) > DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
