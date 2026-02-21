

/* 
  This SQL query calculates the completion rates of voice sessions for rural and urban users, then computes the ratio of rural to urban completion rates.
*/
SELECT
  DATE_TRUNC('month', created_at) AS month,
  COUNT(DISTINCT user_id) AS distinct_users,
  LAG(COUNT(DISTINCT user_id)) OVER (ORDER BY DATE_TRUNC('month', created_at)) AS previous_month_users,
  COUNT(DISTINCT user_id) - LAG(COUNT(DISTINCT user_id)) OVER (ORDER BY DATE_TRUNC('month', created_at)) AS mom_change,
  ROUND(
    ((COUNT(DISTINCT user_id) - LAG(COUNT(DISTINCT user_id)) OVER (ORDER BY DATE_TRUNC('month', created_at))) * 100.0) / 
    LAG(COUNT(DISTINCT user_id)) OVER (ORDER BY DATE_TRUNC('month', created_at)),
    2
  ) AS mom_growth_percentage
FROM 
    upload_voice_sessions_20260220015550
GROUP 
    BY DATE_TRUNC('month', created_at)
ORDER 
    BY month DESC;


/* 
  This SQL query calculates the completion rates of voice sessions for rural and urban users, then computes the ratio of rural to urban completion rates.
*/
SELECT
  DATE_TRUNC('month', current_month.created_at) AS month,
  COUNT(DISTINCT current_month.user_id) AS users_in_month,
  COUNT(DISTINCT CASE 
    WHEN next_month.user_id IS NOT NULL THEN current_month.user_id 
  END) AS users_retained_next_month,
  ROUND(
    (COUNT(DISTINCT CASE 
      WHEN next_month.user_id IS NOT NULL THEN current_month.user_id 
    END) * 100.0) / 
    COUNT(DISTINCT current_month.user_id),
    2
  ) AS retention_rate_percentage
FROM 
    upload_voice_sessions_20260220015550 AS current_month
LEFT JOIN 
    upload_voice_sessions_20260220015550 AS next_month
  ON current_month.user_id = next_month.user_id
  AND DATE_TRUNC('month', next_month.created_at) = DATE_TRUNC('month', current_month.created_at) + INTERVAL 1 MONTH
GROUP BY 
    DATE_TRUNC('month', current_month.created_at)
ORDER BY 
    month DESC;


/* 
  This SQL query calculates the percentage of applications submitted through voice channels compared to total applications.
*/
SELECT
  SUM(CASE WHEN channel = 'voice' THEN 1 ELSE 0 END) AS voice_applications_count,
  COUNT(*) AS total_applications_count,
  ROUND(
    (SUM(CASE WHEN channel = 'voice' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*),
    2
  ) AS voice_applications_percentage
FROM 
	upload_applications_2026022001564;


/* 
  This SQL query calculates the percentage of applications submitted through voice channels compared to total applications, grouped by month.
*/
SELECT
  DATE_TRUNC('month', submitted_at) AS month,
  SUM(CASE WHEN channel = 'voice' THEN 1 ELSE 0 END) AS voice_applications_count,
  COUNT(*) AS total_applications_count,
  ROUND(
    (SUM(CASE WHEN channel = 'voice' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*),
    2
  ) AS voice_applications_percentage
FROM 
    upload_applications_20260220015647
GROUP BY 
    DATE_TRUNC('month', submitted_at)
ORDER BY 
    month DESC;


/* 
  This SQL query calculates the percentage of applications submitted through voice channels compared to total applications, grouped by month and user demographics (region and disability status).
*/
SELECT
  -- u.region,
  -- u.disability_flag,
  DATE_TRUNC('month', a.submitted_at) AS month,
  SUM(CASE WHEN a.channel = 'voice' THEN 1 ELSE 0 END) AS voice_applications_count,
  COUNT(*) AS total_applications_count,
  ROUND(
    (SUM(CASE WHEN a.channel = 'voice' THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*),
    2
  ) AS voice_applications_percentage
FROM 
	upload_applications_20260220015647 a
JOIN 
	upload_users_20260220015436 u ON a.user_id = u.user_id
-- WHERE	
-- 	region = 'rural'
GROUP BY 
	DATE_TRUNC('month', a.submitted_at)
	-- ,u.region
	-- ,u.disability_flag
ORDER BY 
	-- u.region,
	month ASC
	-- voice_applications_percentage DESC;


/* 
  This SQL query calculates the average number of applications submitted per user for voice channels.
*/
SELECT
  COUNT(*) AS total_applications_count,
  COUNT(DISTINCT a.user_id) AS distinct_users_count,
  ROUND(
    COUNT(*) * 1.0 / COUNT(DISTINCT a.user_id),
    2
  ) AS applications_per_user
FROM 
  upload_applications_20260220015647 a
WHERE
	channel = 'voice'
    AND status = 'completed'


/* 
  This SQL query calculates the percentage of new users (users who had their first session in the current month) among all active users for each month.
*/
WITH first_sessions AS (
  SELECT
    user_id,
    MIN(created_at) AS first_session_date
  FROM upload_voice_sessions_20260220015550
  GROUP BY user_id
)
SELECT
  DATE_TRUNC('month', vs.created_at) AS month,
  COUNT(DISTINCT CASE 
    WHEN DATE_TRUNC('month', vs.created_at) = DATE_TRUNC('month', fs.first_session_date) 
    THEN vs.user_id 
  END) AS new_users_count,
  COUNT(DISTINCT vs.user_id) AS total_active_users_count,
  ROUND(
    (COUNT(DISTINCT CASE 
      WHEN DATE_TRUNC('month', vs.created_at) = DATE_TRUNC('month', fs.first_session_date) 
      THEN vs.user_id 
    END) * 100.0) / 
    COUNT(DISTINCT vs.user_id),
    2
  ) AS new_users_percentage
FROM 
  upload_voice_sessions_20260220015550 vs
LEFT JOIN 
  first_sessions fs ON vs.user_id = fs.user_id
GROUP BY 
  DATE_TRUNC('month', vs.created_at)
ORDER BY 
  month DESC;