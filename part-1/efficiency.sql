/* 
  This SQL query calculates the overall completion rate of voice sessions for all users, regardless of disability status. 
  It counts the number of completed sessions and total sessions, then computes the completion rate as a percentage.
*/
SELECT
  SUM(CASE WHEN vs.final_outcome = 'completed' THEN 1 ELSE 0 END) AS completed_sessions_count,
  COUNT(*) AS total_sessions_count,
  ROUND(
    (SUM(CASE WHEN vs.final_outcome = 'completed' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*),
    2
  ) AS overall_completion_rate
FROM upload_voice_sessions_20260220015550 vs
JOIN upload_users_20260220015436 u ON vs.user_id = u.user_id



/* 
  This SQL query compares the completion rates of voice sessions between rural and urban users. 
  It calculates the completion rate for each group and then computes the ratio of rural to urban completion rates.
*/
SELECT
  SUM(CASE WHEN m.recovery_success = true THEN 1 ELSE 0 END) AS sessions_with_recovery_success,
  COUNT(DISTINCT m.session_id) AS sessions_with_any_error,
  ROUND(
    (SUM(CASE WHEN m.recovery_success = true THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(DISTINCT m.session_id),
    2
  ) AS recovery_success_rate_pct
FROM 
	upload_voice_ai_metrics_20260220015601 m
WHERE m.session_id IN (
  SELECT 
  	DISTINCT vt.session_id
  FROM 
  	upload_voice_turns_20260220015613 vt
  WHERE 
  	vt.error_type IN ('misunderstanding', 'silence', 'noise')
);



/* 
  This SQL query calculates the completion rate of voice sessions for first-time digital users. 
  It counts the number of completed sessions and total sessions for first-time users, then computes the completion rate as a percentage.
*/
SELECT
  COUNT(*) AS completed_applications_count,
  AVG(time_to_submit_sec) AS avg_time_to_submit_sec,
  MIN(time_to_submit_sec) AS min_time_to_submit_sec,
  MAX(time_to_submit_sec) AS max_time_to_submit_sec,
  ROUND(AVG(time_to_submit_sec) / 60.0, 2) AS avg_time_to_submit_min,
  ROUND(MIN(time_to_submit_sec) / 60.0, 2) AS min_time_to_submit_min,
  ROUND(MAX(time_to_submit_sec) / 60.0, 2) AS max_time_to_submit_min
FROM upload_applications_20260220015647
WHERE status = 'completed';


/* 
  This SQL query calculates the overall completion rate of voice sessions for all users, regardless of disability status. 
  It counts the number of completed sessions and total sessions, then computes the completion rate as a percentage.
*/
SELECT
  COUNT(*) AS completed_applications_count,
  AVG(time_to_submit_sec) AS avg_time_to_submit_sec,
  MIN(time_to_submit_sec) AS min_time_to_submit_sec,
  MAX(time_to_submit_sec) AS max_time_to_submit_sec,
  ROUND(AVG(time_to_submit_sec) / 60.0, 2) AS avg_time_to_submit_min,
  ROUND(MIN(time_to_submit_sec) / 60.0, 2) AS min_time_to_submit_min,
  ROUND(MAX(time_to_submit_sec) / 60.0, 2) AS max_time_to_submit_min
FROM upload_applications_20260220015647
WHERE status = 'completed' AND channel = 'voice';


/* 
  This SQL query analyzes the ratio of silence turns in voice sessions for rural users with disabilities. 
  It counts the number of silence turns and total turns, then computes the ratio of silence turns as a percentage.
*/
SELECT
  SUM(CASE WHEN error_type != '' AND error_type IS NOT NULL THEN 1 ELSE 0 END) AS turns_with_error_count,
  COUNT(*) AS total_turns_count,
  ROUND(
    (SUM(CASE WHEN error_type != '' AND error_type IS NOT NULL THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*),
    2
  ) AS error_turns_percentage
FROM upload_voice_turns_20260220015613;


/* 
  This SQL query calculates the failure rates of voice applications for different service codes. 
  It counts the number of failed applications and total applications for each service code, then computes the failure rate as a percentage.
*/
SELECT
  service_code,
  SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_applications_count,
  COUNT(*) AS total_applications_count,
  ROUND(
    (SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*),
    2
  ) AS failure_rate_percentage
FROM upload_applications_20260220015647
WHERE channel = 'voice'
GROUP BY service_code
ORDER BY failure_rate_percentage DESC;
