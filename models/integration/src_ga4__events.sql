{{-
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__INTEGRATION', 'true') == 'true',
        tags = ['dbt_package_ga4', 'integration'],
        materialized = 'view'
    )
-}}


WITH raw AS (

{%- set datasets = env_var('DBT_PACKAGE_GA4__DATASETS', '') -%}

{%- if datasets|length == 0 %}

    SELECT
        events._TABLE_SUFFIX AS TABLE_SUFFIX,
        events.*
    FROM
        {{ source('dbt_package_ga4', 'events') }} AS events
    WHERE
        events.stream_id IN UNNEST({{ env_var('DBT_PACKAGE_GA4__STREAM_ID') }})

{%- else %}

    {%- set datasets = datasets.split(',') -%}

    {%- for dataset in datasets %}

    SELECT
        events._TABLE_SUFFIX AS TABLE_SUFFIX,
        events.*
    FROM
        `{{ env_var('DBT_PACKAGE_GA4__PROJECT') }}`.`{{ dataset }}`.`events_*` AS events
    WHERE
        events.stream_id IN UNNEST({{ env_var('DBT_PACKAGE_GA4__STREAM_ID') }})

        {%- if not loop.last %}

    UNION ALL

        {%- endif %}

    {%- endfor %}

{%- endif %}
)

SELECT * FROM raw
