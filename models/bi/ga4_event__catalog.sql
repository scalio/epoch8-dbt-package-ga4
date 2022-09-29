{{
    config(
        enabled=false,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['ga4_event_name', 'ga4_event_platform'],
        cluster_by = ['ga4_event_name', 'ga4_event_platform']
    )
}}


-- WITH ga4_event_timestamp AS (
--     SELECT
--         ga4_event_timestamp.ga4_event_id,
--         ga4_event_timestamp.ga4_event_timestamp
--     FROM
--         {{ ref('attr__ga4_event__ga4_event_timestamp') }} AS ga4_event_timestamp
    
--     {% if is_incremental() %}
--     {% set max_partition_date = macro__get_max_partition_date(this.schema, this.table) %}
--     WHERE
--         ga4_event_timestamp.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
--     {% endif %}
-- ),

WITH ga4_event_name AS (
    SELECT
        ga4_event_name.ga4_event_id,
        ga4_event_name.ga4_event_name
    FROM
        {{ ref('attr__ga4_event__ga4_event_name') }} AS ga4_event_name
    
    {% if is_incremental() %}
    WHERE
        ga4_event_name.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

ga4_event_platform AS (
    SELECT
        ga4_event_platform.ga4_event_id,
        ga4_event_platform.ga4_event_platform
    FROM
        {{ ref('attr__ga4_event__ga4_event_platform') }} AS ga4_event_platform
    
    {% if is_incremental() %}
    WHERE
        ga4_event_platform.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}
),

t1 AS (
    SELECT DISTINCT
        -- TO_HEX(
        --     MD5(
        --         SAFE_CAST(ga4_event_name.ga4_event_name AS STRING)
        --         )
        --     ) AS ga4_event_id,
        -- MIN(TIMESTAMP_MICROS(events.event_timestamp)) AS ga4_event_created_timestamp,
        -- MAX(TIMESTAMP_MICROS(events.event_timestamp)) AS ga4_event_updated_timestamp,
        ga4_event_name.ga4_event_name,
        ga4_event_platform.ga4_event_platform
    FROM
        {{ ref('anchor__ga4_event') }} AS ga4_event
        -- LEFT JOIN ga4_event_timestamp
        --     ON ga4_event_timestamp.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN ga4_event_name
            ON ga4_event_name.ga4_event_id = ga4_event.ga4_event_id
        LEFT JOIN ga4_event_platform
            ON ga4_event_platform.ga4_event_id = ga4_event.ga4_event_id
    
    {% if is_incremental() %}
    WHERE
        ga4_event.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} + 1 DAY)
    {% endif %}

    -- GROUP BY
    --     ga4_event_name.ga4_event_name
),

t2 AS (
    SELECT
    --     t1.ga4_event_id,
    --     COALESCE((
    --         SELECT
    --             this.ga4_event_created_timestamp
    --         FROM
    --             {{ this }} AS this
    --         WHERE
    --             this.ga4_event_id = t1.ga4_event_id
    --         LIMIT 1
    --     ), t1.ga4_event_created_timestamp) AS ga4_event_created_timestamp,
    --     t1.ga4_event_updated_timestamp,
        t1.ga4_event_name,
    --     CAST(NULL AS STRING) AS ga4_event_platform,
        t1.ga4_event_platform
    --     CAST(NULL AS STRING) AS ga4_funnel_name,
    --     CAST(NULL AS STRING) AS ga4_funnel_step
    FROM
        t1
),

final AS (
    SELECT
        t1.ga4_event_name,
        t1.ga4_event_platform
    FROM
        t1
)

SELECT * FROM final

    -- {% if is_incremental() %}
    -- WHERE
    --     final.ga4_event_appearance_timestamp < COALESCE((
    --         SELECT
    --             this.ga4_event_appearance_timestamp
    --         FROM
    --             {{ this }} AS this
    --         WHERE
    --             this.ga4_event_name = final.ga4_event_name
    --     ), TIMESTAMP(CURRENT_DATE()))
    -- {% endif %}
