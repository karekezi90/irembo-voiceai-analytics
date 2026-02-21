-- Side-by-side comparison of first-time vs returning user outcomes
SELECT
  is_first_time_user,
  region,
  COUNT(*) AS sessions,
  COUNT(DISTINCT user_id) AS unique_users,
  ROUND(AVG(CAST(is_completed AS Nullable(Float32))) * 100, 1) AS completion_rate_pct,
  ROUND(AVG(CAST(is_abandoned AS Nullable(Float32))) * 100, 1) AS abandonment_rate_pct,
  ROUND(AVG(CAST(is_escalated AS Nullable(Float32))) * 100, 1) AS escalation_rate_pct,
  ROUND(AVG(misunderstanding_rate) * 100, 1) AS avg_misunderstanding_pct,
  ROUND(AVG(silence_rate) * 100, 1) AS avg_silence_pct,
  ROUND(AVG(repeat_intent_rate_pct), 1) AS avg_repeat_intent_pct,
  ROUND(
    SUM(CASE WHEN is_recovered = 1 AND had_errors = 1 THEN 1.0 ELSE 0 END) / nullIf(SUM(CASE WHEN had_errors = 1 THEN 1 ELSE 0 END), 0) * 100,
    1
  ) AS error_recovery_rate_pct /* Recovery rate only among sessions that had errors */
FROM upload_fact_voice_ai_sessions_20260221143032
GROUP BY
  is_first_time_user,
  region
ORDER BY
  is_first_time_user DESC,
  region;

-- First-time user cohort retention proxy:
-- How many users who had their first session in month N returned in month N+1?
WITH user_cohorts AS (
  SELECT
    user_id,
    MIN(session_month) AS first_session_month,
    MAX(session_month) AS last_session_month
  FROM upload_fact_voice_ai_sessions_20260221143032
  WHERE
    is_first_time_user = 1
  GROUP BY
    user_id
), cohort_with_retention AS (
  SELECT
    user_id,
    first_session_month,
    CASE WHEN last_session_month > first_session_month THEN 1 ELSE 0 END AS returned_next_month
  FROM user_cohorts
)
SELECT
  first_session_month,
  COUNT(DISTINCT user_id) AS new_users,
  COUNT(DISTINCT CASE WHEN returned_next_month = 1 THEN user_id END) AS returned,
  ROUND(
    COUNT(DISTINCT CASE WHEN returned_next_month = 1 THEN user_id END) * 100.0 / nullIf(COUNT(DISTINCT user_id), 0),
    1
  ) AS retention_rate_pct
FROM cohort_with_retention
GROUP BY
  first_session_month
ORDER BY
  first_session_month