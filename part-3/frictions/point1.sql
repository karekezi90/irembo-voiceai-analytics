-- Error type breakdown across all sessions
-- Uses aggregated turn columns pre-computed in the fact table
WITH aggregated_data AS (
  SELECT
    SUM(misunderstanding_turns) AS misunderstanding_turns,
    SUM(silence_turns) AS silence_turns,
    SUM(noise_turns) AS noise_turns,
    SUM(total_error_turns) AS total_error_turns,
    SUM(total_turns) AS total_turns
  FROM upload_fact_voice_ai_sessions_20260221143032
)
SELECT
  misunderstanding_turns,
  silence_turns,
  noise_turns,
  total_error_turns,
  total_turns,
  ROUND(total_error_turns * 100.0 / total_turns, 1) AS error_rate_pct,
  ROUND(misunderstanding_turns * 100.0 / total_turns, 1) AS misunderstanding_pct,
  ROUND(silence_turns * 100.0 / total_turns, 1) AS silence_pct,
  ROUND(noise_turns * 100.0 / total_turns, 1) AS noise_pct
FROM aggregated_data

-- Monthly trend — is the error rate improving?
WITH monthly_aggregates AS (
  SELECT
    session_month,
    SUM(total_turns) AS total_turns,
    SUM(total_error_turns) AS error_turns,
    SUM(misunderstanding_turns) AS misunderstanding_turns,
    SUM(silence_turns) AS silence_turns
  FROM upload_fact_voice_ai_sessions_20260221143032
  GROUP BY
    session_month
)
SELECT
  session_month,
  total_turns,
  error_turns,
  ROUND(error_turns * 100.0 / nullIf(total_turns, 0), 1) AS turn_error_rate_pct,
  ROUND(misunderstanding_turns * 100.0 / nullIf(total_turns, 0), 1) AS misunderstanding_pct,
  ROUND(silence_turns * 100.0 / nullIf(total_turns, 0), 1) AS silence_pct
FROM monthly_aggregates
ORDER BY
  session_month