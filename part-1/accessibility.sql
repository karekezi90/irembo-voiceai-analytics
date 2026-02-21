/* 
  This SQL query calculates the completion rate of voice sessions for users with disabilities. 
  It counts the number of completed sessions and total sessions for disabled users, then computes the completion rate as a percentage.
*/

SELECT
  SUM(CASE WHEN vs.final_outcome = 'completed' THEN 1 ELSE 0 END) AS completed_sessions_count,
  COUNT(*) AS total_sessions_count,
  ROUND(
    (SUM(CASE WHEN vs.final_outcome = 'completed' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*),
    2
  ) AS completion_rate_disabled_users
FROM upload_voice_sessions_20260220015550 vs
JOIN upload_users_20260220015436 u ON vs.user_id = u.user_id
WHERE u.disability_flag = true;


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
  ROUND(
    (SUM(CASE WHEN vs.final_outcome = 'completed' AND u.region = 'rural' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(CASE WHEN u.region = 'rural' THEN 1 END),
    2
  ) AS completion_rate_rural,
  ROUND(
    (SUM(CASE WHEN vs.final_outcome = 'completed' AND u.region = 'urban' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(CASE WHEN u.region = 'urban' THEN 1 END),
    2
  ) AS completion_rate_urban,
  ROUND(
    (SUM(CASE WHEN vs.final_outcome = 'completed' AND u.region = 'rural' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(CASE WHEN u.region = 'rural' THEN 1 END)
    /
    ((SUM(CASE WHEN vs.final_outcome = 'completed' AND u.region = 'urban' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(CASE WHEN u.region = 'urban' THEN 1 END)),
    2
  ) AS completion_rate_ratio_rural_to_urban
FROM upload_voice_sessions_20260220015550 vs
JOIN upload_users_20260220015436 u ON vs.user_id = u.user_id;


/* 
  This SQL query calculates the completion rate of voice sessions for first-time digital users. 
  It counts the number of completed sessions and total sessions for first-time users, then computes the completion rate as a percentage.
*/
SELECT
  SUM(CASE WHEN vs.final_outcome = 'completed' THEN 1 ELSE 0 END) AS completed_sessions_count,
  COUNT(*) AS total_sessions_count,
  ROUND(
    (SUM(CASE WHEN vs.final_outcome = 'completed' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*),
    2
  ) AS completion_rate_first_time_users
FROM 
	upload_voice_sessions_20260220015550 vs
JOIN 
	upload_users_20260220015436 u ON vs.user_id = u.user_id
WHERE 
	u.first_time_digital_user = true;

/* 
  This SQL query analyzes the ratio of silence turns in voice sessions for rural users with disabilities. 
  It counts the number of silence turns and total turns, then computes the ratio of silence turns as a percentage.
*/ 
SELECT
  u.region,
  u.disability_flag,
  u.first_time_digital_user,
  SUM(CASE WHEN vt.error_type = 'silence' THEN 1 ELSE 0 END) AS silence_turns_count,
  COUNT(*) AS total_turns_count,
  ROUND(
    (SUM(CASE WHEN vt.error_type = 'silence' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*),
    2
  ) AS silence_turns_ratio
FROM 
	upload_voice_turns_20260220015613 vt
JOIN 
	upload_voice_sessions_20260220015550 vs ON vt.session_id = vs.session_id
JOIN 
	upload_users_20260220015436 u ON vs.user_id = u.user_id
WHERE
	u.region = 'rural'
	AND u.disability_flag = true
GROUP BY 
	u.region, u.disability_flag, u.first_time_digital_user
ORDER BY 
	u.region, u.disability_flag, u.first_time_digital_user


/* 
  This SQL query calculates the escalation rate of voice sessions for users with disabilities or first-time digital users. 
  It counts the number of transferred sessions and total sessions for these users, then computes the escalation rate as a percentage.
*/
SELECT
  SUM(CASE WHEN vs.final_outcome = 'transferred' THEN 1 ELSE 0 END) AS escalated_sessions_count,
  COUNT(*) AS total_sessions_count,
  ROUND(
    (SUM(CASE WHEN vs.final_outcome = 'transferred' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*),
    2
  ) AS escalation_rate
FROM 
	upload_voice_sessions_20260220015550 vs
JOIN 
	upload_users_20260220015436 u ON vs.user_id = u.user_id
WHERE 
	u.disability_flag = true OR u.first_time_digital_user = true;


/* 
  This SQL query calculates the escalation rate of voice sessions specifically for vulnerable users (those with disabilities or first-time digital users). 
  It counts the number of escalated sessions and total sessions for these vulnerable users, then computes the escalation rate as a percentage.
*/
WITH vulnerable_sessions AS (
    SELECT DISTINCT s.session_id
    FROM upload_voice_sessions_20260220015550 s
    JOIN upload_users_20260220015436 u ON s.user_id = u.user_id
    WHERE u.disability_flag        = 'yes'
       OR u.first_time_digital_user = 'yes'
),

escalation_counts AS (
    SELECT
        COUNT(*) AS total_vulnerable_sessions,
        COUNT(CASE WHEN m.escalation_flag = 'yes' THEN 1 END) AS escalated_sessions,
        ROUND(
            COUNT(CASE WHEN m.escalation_flag = 'yes' THEN 1 END)
            * 100.0 / COUNT(*), 1
        ) AS escalation_rate_pct
    FROM vulnerable_sessions vs
    JOIN upload_voice_ai_metrics_20260220015601 m ON vs.session_id = m.session_id
)

SELECT * FROM escalation_counts;
