{{-
    config(
        enabled = env_var('DBT_PACKAGE_GA4__ENABLE__BI', 'false') == 'true',
        tags = ['dbt_package_ga4', 'bi'],
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ga4_session_id',
        partition_by = {
            "field": "ga4_session_appearance_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = 'ga4_session_id'
    )
-}}


WITH ga4_session_geo_continent AS (
    SELECT
        ga4_session_geo_continent.ga4_session_id,
        ga4_session_geo_continent.ga4_session_geo_continent
    FROM
        {{ ref('attr__ga4_session__ga4_session_geo_continent') }} AS ga4_session_geo_continent
    
    {%- if is_incremental() %}
    {%- set max_partition_date = macro__get_max_partition_date(this.schema, this.table) %}
    WHERE
        ga4_session_geo_continent.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_geo_sub_continent AS (
    SELECT
        ga4_session_geo_sub_continent.ga4_session_id,
        ga4_session_geo_sub_continent.ga4_session_geo_sub_continent
    FROM
        {{ ref('attr__ga4_session__ga4_session_geo_sub_continent') }} AS ga4_session_geo_sub_continent
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_geo_sub_continent.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_geo_country AS (
    SELECT
        ga4_session_geo_country.ga4_session_id,
        ga4_session_geo_country.ga4_session_geo_country
    FROM
        {{ ref('attr__ga4_session__ga4_session_geo_country') }} AS ga4_session_geo_country
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_geo_country.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_geo_region AS (
    SELECT
        ga4_session_geo_region.ga4_session_id,
        ga4_session_geo_region.ga4_session_geo_region
    FROM
        {{ ref('attr__ga4_session__ga4_session_geo_region') }} AS ga4_session_geo_region
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_geo_region.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_geo_city AS (
    SELECT
        ga4_session_geo_city.ga4_session_id,
        ga4_session_geo_city.ga4_session_geo_city
    FROM
        {{ ref('attr__ga4_session__ga4_session_geo_city') }} AS ga4_session_geo_city
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_geo_city.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_geo_metro AS (
    SELECT
        ga4_session_geo_metro.ga4_session_id,
        ga4_session_geo_metro.ga4_session_geo_metro
    FROM
        {{ ref('attr__ga4_session__ga4_session_geo_metro') }} AS ga4_session_geo_metro
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_geo_metro.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_traffic_source_name AS (
    SELECT
        ga4_session_traffic_source_name.ga4_session_id,
        ga4_session_traffic_source_name.ga4_session_traffic_source_name
    FROM
        {{ ref('attr__ga4_session__ga4_session_traffic_source_name') }} AS ga4_session_traffic_source_name
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_traffic_source_name.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_traffic_source_medium AS (
    SELECT
        ga4_session_traffic_source_medium.ga4_session_id,
        ga4_session_traffic_source_medium.ga4_session_traffic_source_medium
    FROM
        {{ ref('attr__ga4_session__ga4_session_traffic_source_medium') }} AS ga4_session_traffic_source_medium
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_traffic_source_medium.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_traffic_source_source AS (
    SELECT
        ga4_session_traffic_source_source.ga4_session_id,
        ga4_session_traffic_source_source.ga4_session_traffic_source_source
    FROM
        {{ ref('attr__ga4_session__ga4_session_traffic_source_source') }} AS ga4_session_traffic_source_source
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_traffic_source_source.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_category AS (
    SELECT
        ga4_session_device_category.ga4_session_id,
        ga4_session_device_category.ga4_session_device_category
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_category') }} AS ga4_session_device_category
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_category.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_mobile_brand_name AS (
    SELECT
        ga4_session_device_mobile_brand_name.ga4_session_id,
        ga4_session_device_mobile_brand_name.ga4_session_device_mobile_brand_name
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_mobile_brand_name') }} AS ga4_session_device_mobile_brand_name
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_mobile_brand_name.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_mobile_model_name AS (
    SELECT
        ga4_session_device_mobile_model_name.ga4_session_id,
        ga4_session_device_mobile_model_name.ga4_session_device_mobile_model_name
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_mobile_model_name') }} AS ga4_session_device_mobile_model_name
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_mobile_model_name.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_mobile_marketing_name AS (
    SELECT
        ga4_session_device_mobile_marketing_name.ga4_session_id,
        ga4_session_device_mobile_marketing_name.ga4_session_device_mobile_marketing_name
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_mobile_marketing_name') }} AS ga4_session_device_mobile_marketing_name
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_mobile_marketing_name.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_mobile_os_hardware_model AS (
    SELECT
        ga4_session_device_mobile_os_hardware_model.ga4_session_id,
        ga4_session_device_mobile_os_hardware_model.ga4_session_device_mobile_os_hardware_model
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_mobile_os_hardware_model') }} AS ga4_session_device_mobile_os_hardware_model
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_mobile_os_hardware_model.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_operating_system AS (
    SELECT
        ga4_session_device_operating_system.ga4_session_id,
        ga4_session_device_operating_system.ga4_session_device_operating_system
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_operating_system') }} AS ga4_session_device_operating_system
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_operating_system.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_operating_system_version AS (
    SELECT
        ga4_session_device_operating_system_version.ga4_session_id,
        ga4_session_device_operating_system_version.ga4_session_device_operating_system_version
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_operating_system_version') }} AS ga4_session_device_operating_system_version
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_operating_system_version.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_vendor_id AS (
    SELECT
        ga4_session_device_vendor_id.ga4_session_id,
        ga4_session_device_vendor_id.ga4_session_device_vendor_id
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_vendor_id') }} AS ga4_session_device_vendor_id
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_vendor_id.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_advertising_id AS (
    SELECT
        ga4_session_device_advertising_id.ga4_session_id,
        ga4_session_device_advertising_id.ga4_session_device_advertising_id
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_advertising_id') }} AS ga4_session_device_advertising_id
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_advertising_id.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_language AS (
    SELECT
        ga4_session_device_language.ga4_session_id,
        ga4_session_device_language.ga4_session_device_language
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_language') }} AS ga4_session_device_language
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_language.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_time_zone_offset_seconds AS (
    SELECT
        ga4_session_device_time_zone_offset_seconds.ga4_session_id,
        ga4_session_device_time_zone_offset_seconds.ga4_session_device_time_zone_offset_seconds
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_time_zone_offset_seconds') }} AS ga4_session_device_time_zone_offset_seconds
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_time_zone_offset_seconds.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_is_limited_ad_tracking AS (
    SELECT
        ga4_session_device_is_limited_ad_tracking.ga4_session_id,
        ga4_session_device_is_limited_ad_tracking.ga4_session_device_is_limited_ad_tracking
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_is_limited_ad_tracking') }} AS ga4_session_device_is_limited_ad_tracking
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_is_limited_ad_tracking.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_web_info_browser AS (
    SELECT
        ga4_session_device_web_info_browser.ga4_session_id,
        ga4_session_device_web_info_browser.ga4_session_device_web_info_browser
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_web_info_browser') }} AS ga4_session_device_web_info_browser
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_web_info_browser.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_web_info_browser_version AS (
    SELECT
        ga4_session_device_web_info_browser_version.ga4_session_id,
        ga4_session_device_web_info_browser_version.ga4_session_device_web_info_browser_version
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_web_info_browser_version') }} AS ga4_session_device_web_info_browser_version
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_web_info_browser_version.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_device_web_info_hostname AS (
    SELECT
        ga4_session_device_web_info_hostname.ga4_session_id,
        ga4_session_device_web_info_hostname.ga4_session_device_web_info_hostname
    FROM
        {{ ref('attr__ga4_session__ga4_session_device_web_info_hostname') }} AS ga4_session_device_web_info_hostname
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_device_web_info_hostname.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_app_info_id AS (
    SELECT
        ga4_session_app_info_id.ga4_session_id,
        ga4_session_app_info_id.ga4_session_app_info_id
    FROM
        {{ ref('attr__ga4_session__ga4_session_app_info_id') }} AS ga4_session_app_info_id
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_app_info_id.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_app_info_firebase_app_id AS (
    SELECT
        ga4_session_app_info_firebase_app_id.ga4_session_id,
        ga4_session_app_info_firebase_app_id.ga4_session_app_info_firebase_app_id
    FROM
        {{ ref('attr__ga4_session__ga4_session_app_info_firebase_app_id') }} AS ga4_session_app_info_firebase_app_id
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_app_info_firebase_app_id.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_app_info_install_source AS (
    SELECT
        ga4_session_app_info_install_source.ga4_session_id,
        ga4_session_app_info_install_source.ga4_session_app_info_install_source
    FROM
        {{ ref('attr__ga4_session__ga4_session_app_info_install_source') }} AS ga4_session_app_info_install_source
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_app_info_install_source.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_app_info_version AS (
    SELECT
        ga4_session_app_info_version.ga4_session_id,
        ga4_session_app_info_version.ga4_session_app_info_version
    FROM
        {{ ref('attr__ga4_session__ga4_session_app_info_version') }} AS ga4_session_app_info_version
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_app_info_version.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

ga4_session_app_platform AS (
    SELECT
        ga4_session_app_platform.ga4_session_id,
        ga4_session_app_platform.ga4_session_app_platform
    FROM
        {{ ref('attr__ga4_session__ga4_session_app_platform') }} AS ga4_session_app_platform
    
    {%- if is_incremental() %}
    WHERE
        ga4_session_app_platform.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

t1 AS (
    SELECT
        ga4_session.ga4_session_id,
        ga4_session.ga4_session_ga4_id,
        ga4_session.ga4_session_appearance_timestamp,
        ga4_session_geo_continent.ga4_session_geo_continent,
        ga4_session_geo_sub_continent.ga4_session_geo_sub_continent,
        ga4_session_geo_country.ga4_session_geo_country,
        ga4_session_geo_region.ga4_session_geo_region,
        ga4_session_geo_city.ga4_session_geo_city,
        ga4_session_geo_metro.ga4_session_geo_metro,
        ga4_session_traffic_source_name.ga4_session_traffic_source_name,
        ga4_session_traffic_source_medium.ga4_session_traffic_source_medium,
        ga4_session_traffic_source_source.ga4_session_traffic_source_source,
        ga4_session_device_category.ga4_session_device_category,
        ga4_session_device_mobile_brand_name.ga4_session_device_mobile_brand_name,
        ga4_session_device_mobile_model_name.ga4_session_device_mobile_model_name,
        ga4_session_device_mobile_marketing_name.ga4_session_device_mobile_marketing_name,
        ga4_session_device_mobile_os_hardware_model.ga4_session_device_mobile_os_hardware_model,
        ga4_session_device_operating_system.ga4_session_device_operating_system,
        ga4_session_device_operating_system_version.ga4_session_device_operating_system_version,
        ga4_session_device_vendor_id.ga4_session_device_vendor_id,
        ga4_session_device_advertising_id.ga4_session_device_advertising_id,
        ga4_session_device_language.ga4_session_device_language,
        ga4_session_device_time_zone_offset_seconds.ga4_session_device_time_zone_offset_seconds,
        ga4_session_device_is_limited_ad_tracking.ga4_session_device_is_limited_ad_tracking,
        ga4_session_device_web_info_browser.ga4_session_device_web_info_browser,
        ga4_session_device_web_info_browser_version.ga4_session_device_web_info_browser_version,
        ga4_session_device_web_info_hostname.ga4_session_device_web_info_hostname,
        ga4_session_app_info_id.ga4_session_app_info_id,
        ga4_session_app_info_firebase_app_id.ga4_session_app_info_firebase_app_id,
        ga4_session_app_info_install_source.ga4_session_app_info_install_source,
        ga4_session_app_info_version.ga4_session_app_info_version,
        ga4_session_app_platform.ga4_session_app_platform
    FROM
        {{ ref('anchor__ga4_session') }} AS ga4_session
        LEFT JOIN ga4_session_geo_continent
            ON ga4_session_geo_continent.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_geo_sub_continent
            ON ga4_session_geo_sub_continent.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_geo_country
            ON ga4_session_geo_country.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_geo_region
            ON ga4_session_geo_region.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_geo_city
            ON ga4_session_geo_city.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_geo_metro
            ON ga4_session_geo_metro.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_traffic_source_name
            ON ga4_session_traffic_source_name.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_traffic_source_medium
            ON ga4_session_traffic_source_medium.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_traffic_source_source
            ON ga4_session_traffic_source_source.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_category
            ON ga4_session_device_category.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_mobile_brand_name
            ON ga4_session_device_mobile_brand_name.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_mobile_model_name
            ON ga4_session_device_mobile_model_name.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_mobile_marketing_name
            ON ga4_session_device_mobile_marketing_name.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_mobile_os_hardware_model
            ON ga4_session_device_mobile_os_hardware_model.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_operating_system
            ON ga4_session_device_operating_system.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_operating_system_version
            ON ga4_session_device_operating_system_version.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_vendor_id
            ON ga4_session_device_vendor_id.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_advertising_id
            ON ga4_session_device_advertising_id.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_language
            ON ga4_session_device_language.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_time_zone_offset_seconds
            ON ga4_session_device_time_zone_offset_seconds.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_is_limited_ad_tracking
            ON ga4_session_device_is_limited_ad_tracking.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_web_info_browser
            ON ga4_session_device_web_info_browser.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_web_info_browser_version
            ON ga4_session_device_web_info_browser_version.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_device_web_info_hostname
            ON ga4_session_device_web_info_hostname.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_app_info_id
            ON ga4_session_app_info_id.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_app_info_firebase_app_id
            ON ga4_session_app_info_firebase_app_id.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_app_info_install_source
            ON ga4_session_app_info_install_source.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_app_info_version
            ON ga4_session_app_info_version.ga4_session_id = ga4_session.ga4_session_id
        LEFT JOIN ga4_session_app_platform
            ON ga4_session_app_platform.ga4_session_id = ga4_session.ga4_session_id
    
    {%- if is_incremental() %}
    WHERE
        ga4_session.ga4_date_partition >= DATE_SUB(DATE('{{ max_partition_date }}'), INTERVAL {{ env_var('DBT_PACKAGE_GA4__INTERVAL_INCREMENTAL') }} DAY)
    {%- endif %}
),

final AS (
    SELECT
        t1.ga4_session_id,
        t1.ga4_session_ga4_id,
        t1.ga4_session_appearance_timestamp,
        t1.ga4_session_geo_continent,
        t1.ga4_session_geo_sub_continent,
        t1.ga4_session_geo_country,
        t1.ga4_session_geo_region,
        t1.ga4_session_geo_city,
        t1.ga4_session_geo_metro,
        t1.ga4_session_traffic_source_name,
        t1.ga4_session_traffic_source_medium,
        t1.ga4_session_traffic_source_source,
        t1.ga4_session_device_category,
        t1.ga4_session_device_mobile_brand_name,
        t1.ga4_session_device_mobile_model_name,
        t1.ga4_session_device_mobile_marketing_name,
        t1.ga4_session_device_mobile_os_hardware_model,
        t1.ga4_session_device_operating_system,
        t1.ga4_session_device_operating_system_version,
        t1.ga4_session_device_vendor_id,
        t1.ga4_session_device_advertising_id,
        t1.ga4_session_device_language,
        t1.ga4_session_device_time_zone_offset_seconds,
        t1.ga4_session_device_is_limited_ad_tracking,
        t1.ga4_session_device_web_info_browser,
        t1.ga4_session_device_web_info_browser_version,
        t1.ga4_session_device_web_info_hostname,
        t1.ga4_session_app_info_id,
        t1.ga4_session_app_info_firebase_app_id,
        t1.ga4_session_app_info_install_source,
        t1.ga4_session_app_info_version,
        t1.ga4_session_app_platform
    FROM
        t1
)

SELECT * FROM final

    {%- if is_incremental() %}
    WHERE
        final.ga4_session_appearance_timestamp < COALESCE((
            SELECT
                this.ga4_session_appearance_timestamp
            FROM
                {{ this }} AS this
            WHERE
                this.ga4_session_id = final.ga4_session_id
        ), TIMESTAMP(CURRENT_DATE()))
    {%- endif %}
