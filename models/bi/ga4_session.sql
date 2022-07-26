{{
    config(
        enabled=false,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            "field": "ga4_session_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}


WITH t1 AS (
    SELECT
        ga4_session.ga4_session_id,
        ga4_session.ga4_session_date,
        -- ga4_session_geo_continent.ga4_session_geo_continent,
        -- ga4_session_geo_sub_continent.ga4_session_geo_sub_continent,
        -- ga4_session_geo_country.ga4_session_geo_country,
        -- ga4_session_geo_region.ga4_session_geo_region,
        -- ga4_session_geo_city.ga4_session_geo_city,
        -- ga4_session_geo_metro.ga4_session_geo_metro,
        ga4_session_geo.ga4_session_geo_continent,
        ga4_session_geo.ga4_session_geo_sub_continent,
        ga4_session_geo.ga4_session_geo_country,
        ga4_session_geo.ga4_session_geo_region,
        ga4_session_geo.ga4_session_geo_city,
        ga4_session_geo.ga4_session_geo_metro
    FROM
        {{ ref('anchor__ga4_session') }} AS ga4_session
        -- LEFT JOIN {# ref('attr__ga4_session__ga4_session_geo_continent') #} AS ga4_session_geo_continent
        --     ON ga4_session_geo_continent.ga4_session_id = ga4_session.ga4_session_id
        -- LEFT JOIN {# ref('attr__ga4_session__ga4_session_geo_sub_continent') #} AS ga4_session_geo_sub_continent
        --     ON ga4_session_geo_sub_continent.ga4_session_id = ga4_session.ga4_session_id
        -- LEFT JOIN {# ref('attr__ga4_session__ga4_session_geo_country') #} AS ga4_session_geo_country
        --     ON ga4_session_geo_country.ga4_session_id = ga4_session.ga4_session_id
        -- LEFT JOIN {# ref('attr__ga4_session__ga4_session_geo_region') #} AS ga4_session_geo_region
        --     ON ga4_session_geo_region.ga4_session_id = ga4_session.ga4_session_id
        -- LEFT JOIN {# ref('attr__ga4_session__ga4_session_geo_city') #} AS ga4_session_geo_city
        --     ON ga4_session_geo_city.ga4_session_id = ga4_session.ga4_session_id
        -- LEFT JOIN {# ref('attr__ga4_session__ga4_session_geo_metro') #} AS ga4_session_geo_metro
        --     ON ga4_session_geo_metro.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN {{ ref('attr__ga4_session__ga4_session_geo') }} AS ga4_session_geo
            ON ga4_session_geo.ga4_session_id = ga4_session.ga4_session_id
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
        WHERE
            ga4_session.ga4_session_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            -- AND ga4_session_geo_continent.ga4_session_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            -- AND ga4_session_geo_sub_continent.ga4_session_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            -- AND ga4_session_geo_country.ga4_session_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            -- AND ga4_session_geo_region.ga4_session_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            -- AND ga4_session_geo_city.ga4_session_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            -- AND ga4_session_geo_metro.ga4_session_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
            AND ga4_session_geo.ga4_session_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
),

final AS (
    SELECT
        t1.ga4_session_id,
        t1.ga4_session_date,
        t1.ga4_session_geo_continent,
        t1.ga4_session_geo_sub_continent,
        t1.ga4_session_geo_country,
        t1.ga4_session_geo_region,
        t1.ga4_session_geo_city,
        t1.ga4_session_geo_metro
    FROM
        t1
)

SELECT * FROM final

    {% if is_incremental() %}
    WHERE
        final.ga4_session_date > DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL {{ var('VAR_INTERVAL_INCREMENTAL') }} DAY)
    {% endif %}
