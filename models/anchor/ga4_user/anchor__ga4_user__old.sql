{{
    config(
        enabled=false,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_user_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}


WITH t1 AS (
    SELECT DISTINCT
        src__events.user_pseudo_id AS ga4_user_id,
        SAFE_CAST(src__events.event_date AS DATE FORMAT 'YYYYMMDD') AS ga4_user_date
    FROM
        {{ ref('src__events') }} AS src__events
    
    {% if is_incremental() %}
    {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
    WHERE
        SAFE_CAST(src__events.event_date AS DATE FORMAT 'YYYYMMDD') > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL 7 DAY)
    {% endif %}
),

t2 AS (
    SELECT
        t1.ga4_user_id,
        t1.ga4_user_date
    FROM
        t1
    WHERE
        t1.ga4_user_id IS NOT NULL
        AND t1.ga4_user_date IS NOT NULL
),

final AS (
    SELECT
        t2.ga4_user_id,
        t2.ga4_user_date
    FROM
        t2
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_user_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL 7 DAY)
    {% endif %}
