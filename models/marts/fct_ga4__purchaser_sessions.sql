{{ config(materialized='table') }}

/*
  GA4 Purchaser Sessions Fact Table
  
  Grain: 1 row per session
  Purpose: Session-level analysis with purchaser flags and detailed attributes
  Notes: Not aggregated - keeps session granularity for flexible analysis
*/

with session_data as (
  select
    -- Session identifiers
    user_pseudo_id,
    session_id,
    session_key,
    session_date,
    session_start_ts,
    session_end_ts,
    session_duration_seconds,
    session_number,
    
    -- Session characteristics
    is_first_session,
    is_bounce,
    is_converted,
    
    -- Platform & Device dimensions (flattened)
    operating_system,
    device.operating_system_version as operating_system_version,
    browser,
    device.web_info.browser_version as browser_version,
    device_category,
    platform,
    device.mobile_brand_name as mobile_brand_name,
    device.mobile_model_name as mobile_model_name,
    device.mobile_marketing_name as mobile_marketing_name,
    device.mobile_os_hardware_model as mobile_os_hardware_model,
    device.language as device_language,
    device.is_limited_ad_tracking as is_limited_ad_tracking,
    device.time_zone_offset_seconds as time_zone_offset_seconds,
    
    -- Geography dimensions
    country,
    region,
    city,
    
    -- Traffic source
    traffic_source,
    traffic_medium,
    traffic_campaign,
    
    -- Landing/Exit
    landing_page,
    exit_page,
    referrer,
    
    -- Funnel event counts (raw)
    product_views,
    add_to_carts,
    checkouts_started,
    purchases,
    
    -- Funnel progression flags
    reached_product_view,
    reached_add_to_cart,
    reached_checkout,
    reached_purchase,
    
    -- Funnel progression metrics
    furthest_funnel_stage,
    furthest_funnel_stage_name,
    drop_off_stage,
    
    -- Revenue
    session_revenue,
    currency,
    
    -- Transaction details
    transaction_ids,
    transaction_count
    
  from {{ ref('rpt_funnel_sessions') }}
)

select
  -- Session identifiers
  session_key,
  user_pseudo_id,
  session_id,
  session_date,
  session_start_ts,
  session_end_ts,
  session_duration_seconds,
  round(session_duration_seconds / 60.0, 2) as session_duration_minutes,
  session_number,
  
  -- Purchaser flags
  is_converted as is_purchaser,
  purchases > 0 as has_purchase_event,
  transaction_count > 0 as has_transaction_id,
  
  -- User type segmentation
  case
    when is_first_session and is_converted then 'First Session Converter'
    when is_first_session and not is_converted then 'First Session Non-Converter'
    when not is_first_session and is_converted then 'Repeat Session Converter'
    when not is_first_session and not is_converted then 'Repeat Session Non-Converter'
  end as user_session_type,
  
  -- Session quality flags
  is_first_session,
  is_bounce,
  session_duration_seconds > 0 as has_engagement,
  session_duration_seconds >= 30 as is_engaged_30s,
  session_duration_seconds >= 60 as is_engaged_1min,
  session_duration_seconds >= 180 as is_engaged_3min,
  
  -- Platform & Device (flattened for CSV export)
  operating_system,
  operating_system_version,
  browser,
  browser_version,
  device_category,
  platform,
  mobile_brand_name,
  mobile_model_name,
  mobile_marketing_name,
  mobile_os_hardware_model,
  device_language,
  is_limited_ad_tracking,
  time_zone_offset_seconds,
  
  -- Geography
  country,
  region,
  city,
  
  -- Traffic attribution
  traffic_source,
  traffic_medium,
  traffic_campaign,
  
  -- User journey
  landing_page,
  exit_page,
  referrer,
  
  -- Funnel metrics (event counts)
  product_views,
  add_to_carts,
  checkouts_started,
  purchases,
  
  -- Funnel progression (boolean flags)
  reached_product_view,
  reached_add_to_cart,
  reached_checkout,
  reached_purchase,
  
  -- Funnel analysis
  furthest_funnel_stage,
  furthest_funnel_stage_name,
  drop_off_stage,
  
  -- Revenue metrics
  session_revenue,
  currency,
  case when session_revenue > 0 then session_revenue else 0 end as revenue_clean,
  
  -- Transaction details (CSV-friendly)
  array_to_string(transaction_ids, ', ') as transaction_ids_list,
  transaction_ids[safe_offset(0)] as first_transaction_id,
  transaction_count,
  
  -- Engagement cohort (for analysis)
  case
    when session_duration_seconds = 0 then '0s (Bounce)'
    when session_duration_seconds <= 10 then '1-10s'
    when session_duration_seconds <= 30 then '11-30s'
    when session_duration_seconds <= 60 then '31-60s'
    when session_duration_seconds <= 180 then '1-3min'
    when session_duration_seconds <= 300 then '3-5min'
    else '5min+'
  end as engagement_cohort

from session_data
