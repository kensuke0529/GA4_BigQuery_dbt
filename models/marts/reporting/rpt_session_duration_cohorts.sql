{{ config(materialized='table') }}

/*
  Session Duration Cohort Analysis
  
  Groups sessions into time-spent buckets to analyze
  correlation between engagement time and conversion.
*/

with session_data as (
  select
    session_key,
    session_duration_seconds,
    is_converted,
    session_revenue,
    landing_page,
    traffic_source,
    device_category
  from {{ ref('rpt_funnel_sessions') }}
),

cohorts as (
  select
    *,
    -- Create time-spent buckets
    case
      when session_duration_seconds = 0 then '0s (Bounce)'
      when session_duration_seconds <= 10 then '1-10s (Quick Visit)'
      when session_duration_seconds <= 30 then '11-30s (Browse)'
      when session_duration_seconds <= 60 then '31-60s (Engaged)'
      when session_duration_seconds <= 180 then '1-3min (Very Engaged)'
      when session_duration_seconds <= 300 then '3-5min (Deep Engagement)'
      else '5min+ (Extended Session)'
    end as duration_cohort,
    -- Order for sorting
    case
      when session_duration_seconds = 0 then 1
      when session_duration_seconds <= 10 then 2
      when session_duration_seconds <= 30 then 3
      when session_duration_seconds <= 60 then 4
      when session_duration_seconds <= 180 then 5
      when session_duration_seconds <= 300 then 6
      else 7
    end as cohort_order
  from session_data
)

select
  -- Dimensions
  landing_page,
  traffic_source,
  device_category,
  duration_cohort,
  cohort_order,
  
  -- Volume
  count(*) as total_sessions,
  count(distinct case when is_converted then session_key end) as converted_sessions,
  
  -- Conversion
  round(safe_divide(
    count(distinct case when is_converted then session_key end),
    count(*)
  ) * 100, 2) as conversion_rate_pct,
  
  -- Revenue
  sum(session_revenue) as total_revenue,
  round(avg(session_revenue), 2) as avg_revenue_per_session,
  
  -- Avg duration within cohort
  round(avg(session_duration_seconds), 2) as avg_duration_seconds,
  round(min(session_duration_seconds), 2) as min_duration_seconds,
  round(max(session_duration_seconds), 2) as max_duration_seconds

from cohorts
group by landing_page, traffic_source, device_category, duration_cohort, cohort_order
order by landing_page, traffic_source, device_category, cohort_order
