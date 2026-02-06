{{ config(materialized='table') }}

/*
  Landing Page Engagement Matrix
  
  Cross-analysis of landing pages and session duration
  to identify high-performing entry points.
*/

with session_data as (
  select
    session_key,
    landing_page,
    session_duration_seconds,
    is_converted,
    session_revenue
  from {{ ref('rpt_funnel_sessions') }}
  where landing_page is not null
),

categorized as (
  select
    landing_page,
    session_duration_seconds,
    is_converted,
    session_revenue,
    session_key,
    -- Simplified engagement levels
    case
      when session_duration_seconds <= 30 then 'Low Engagement (0-30s)'
      when session_duration_seconds <= 120 then 'Medium Engagement (31-120s)'
      else 'High Engagement (120s+)'
    end as engagement_level,
    -- Order for sorting
    case
      when session_duration_seconds <= 30 then 1
      when session_duration_seconds <= 120 then 2
      else 3
    end as engagement_order
  from session_data
)

select
  landing_page,
  engagement_level,
  engagement_order,
  
  -- Volume
  count(*) as sessions,
  
  -- Conversion
  countif(is_converted) as conversions,
  round(safe_divide(countif(is_converted), count(*)) * 100, 2) as conversion_rate_pct,
  
  -- Revenue
  sum(session_revenue) as total_revenue,
  round(avg(session_revenue), 2) as avg_revenue_per_session,
  
  -- Time metrics
  round(avg(session_duration_seconds), 2) as avg_duration_seconds

from categorized
group by landing_page, engagement_level, engagement_order
order by landing_page, engagement_order
