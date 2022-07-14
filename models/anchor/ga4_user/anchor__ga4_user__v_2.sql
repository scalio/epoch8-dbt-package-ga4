{% if var('static_incremental_days', false ) ==  true %}
    {% set partitions_to_replace = [] %}
    {% for i in range(var('static_incremental_days')) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            enabled=false,
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "ga4_user_date",
                "data_type": "date",
                "granularity": "day"
            },
            partitions = partitions_to_replace
        )
    }}
{% else %}
    {{
        config(
            enabled=false,
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "ga4_user_date",
                "data_type": "date",
                "granularity": "day"
            }
        )
    }}
{% endif %}


--BigQuery does not cache wildcard queries that scan across sharded tables which means it's best to materialize the raw event data as a partitioned table so that future queries benefit from caching
WITH t1 AS (
    SELECT DISTINCT
        src__events.user_pseudo_id AS ga4_user_id,
        SAFE_CAST(src__events.event_date AS DATE FORMAT 'YYYYMMDD') AS ga4_user_date
    FROM
        -- {{ ref('src__events') }} AS src__events
        {{ source('ga4', 'events') }} AS src__events
    
    WHERE
        _table_suffix NOT LIKE '%intraday%'
        AND CAST(_table_suffix AS INT64) >= {{ var('start_date') }}
    
    {% if is_incremental() %}
        {% set max_patition_date = macro__get_max_patition_date(this.schema, this.table) %}
        {% if var('static_incremental_days', false ) ==  true %}
            AND PARSE_DATE('%Y%m%d', src__events.event_date) IN ({{ partitions_to_replace | join(',') }})
        {% else %}
            -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max event_date_dt value found in {{this}}
            -- See https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
            AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE_SUB(DATE('{{ max_patition_date }}'), INTERVAL 7 DAY)
        {% endif %} 
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
