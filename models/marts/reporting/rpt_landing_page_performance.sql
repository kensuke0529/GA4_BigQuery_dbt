{{ config(materialized='table') }}

/*
  Landing Page Performance Report
  
  Analyzes which landing pages drive conversion and revenue.
  One row per landing page with performance metrics.
*/

with session_data as (
  select
    session_key,
    landing_page,
    session_duration_seconds,
    is_converted,
    session_revenue,
    reached_product_view,
    reached_add_to_cart,
    reached_checkout,
    reached_purchase
  from {{ ref('rpt_funnel_sessions') }}
  where landing_page is not null
)

select
  landing_page,
  
  -- Volume metrics
  count(*) as total_sessions,
  count(distinct case when is_converted then session_key end) as converted_sessions,
  
  -- Conversion metrics
  round(safe_divide(
    count(distinct case when is_converted then session_key end),
    count(*)
  ) * 100, 2) as conversion_rate_pct,
  
  -- Revenue metrics
  sum(session_revenue) as total_revenue,
  round(safe_divide(sum(session_revenue), count(*)), 2) as revenue_per_session,
  round(safe_divide(
    sum(session_revenue),
    count(distinct case when is_converted then session_key end)
  ), 2) as revenue_per_conversion,
  
  -- Engagement metrics
  round(avg(session_duration_seconds), 2) as avg_session_duration_seconds,
  round(avg(session_duration_seconds) / 60, 2) as avg_session_duration_minutes,
  
  -- Funnel progression
  round(safe_divide(countif(reached_product_view), count(*)) * 100, 2) as pct_reached_product_view,
  round(safe_divide(countif(reached_add_to_cart), count(*)) * 100, 2) as pct_reached_add_to_cart,
  round(safe_divide(countif(reached_checkout), count(*)) * 100, 2) as pct_reached_checkout,
  round(safe_divide(countif(reached_purchase), count(*)) * 100, 2) as pct_completed_purchase

from session_data
group by landing_page
order by total_sessions desc
