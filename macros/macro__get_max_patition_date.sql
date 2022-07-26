{% macro macro__get_max_patition_date(schema, table) %}

    {% set query %}
        SELECT COALESCE(SAFE_CAST(MAX(partition_id) AS DATE FORMAT 'YYYYMMDD'), DATE(CURRENT_DATE())) AS dbt_max_partition
        FROM `{{ schema }}`.`INFORMATION_SCHEMA`.`PARTITIONS`
        WHERE table_name = '{{ table }}' AND partition_id != '__NULL__'
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set result = results.columns[0].values()[0] %}
    {% else %}
        {% set result = [] %}
    {% endif %}

    {{ return(result) }}
    
{% endmacro %}
