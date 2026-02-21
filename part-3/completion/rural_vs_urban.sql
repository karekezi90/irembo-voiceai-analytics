-- Compare completion and friction metrics between rural and urban users
SELECT
  region,
  COUNT(*) AS sessions,
  SUM(CASE WHEN is_completed = 1 THEN 1 ELSE 0 END) AS completed,
  ROUND(AVG(CAST(is_completed AS Nullable(Float32))) * 100, 1) AS completion_rate_pct,
  ROUND(AVG(CAST(is_abandoned AS Nullable(Float32))) * 100, 1) AS abandonment_rate_pct,
  ROUND(AVG(CAST(is_escalated AS Nullable(Float32))) * 100, 1) AS escalation_rate_pct,
  ROUND(AVG(misunderstanding_rate) * 100, 1) AS avg_misunderstanding_pct,
  ROUND(AVG(silence_rate) * 100, 1) AS avg_silence_pct,
  ROUND(
    AVG(CAST(is_completed AS Nullable(Float32))) / nullIf(MAX(AVG(CAST(is_completed AS Nullable(Float32)))) OVER (), 0),
    2
  ) AS parity_ratio /* Parity ratio (rural ÷ urban) computed via window function */
FROM upload_fact_voice_ai_sessions_20260221143032
GROUP BY
  region
ORDER BY
  region