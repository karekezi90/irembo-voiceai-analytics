-- stg_applications_per_session.sql
-- Purpose: Aggregate application outcomes to session level.
-- One session can have multiple application attempts.
-- Input:  900 rows (applications), 646 unique session_ids
-- Output: one row per session_id that has applications

SELECT
    session_id,

    -- Count of application attempts in this session
    COUNT(*) AS total_application_attempts,

    -- Did any attempt succeed?
    MAX(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS had_successful_application,

    -- Counts by status
    COUNT(CASE WHEN status = 'completed' THEN 1 END) AS applications_completed,
    COUNT(CASE WHEN status = 'abandoned' THEN 1 END) AS applications_abandoned,
    COUNT(CASE WHEN status = 'failed'    THEN 1 END) AS applications_failed,

    -- Which services were attempted?
    groupArray(DISTINCT service_code) AS services_attempted,

    -- Primary service (most common one in this session)
    -- Using topK to get the most frequent service
    arrayElement(topK(1)(service_code), 1) AS primary_service_code,

    -- Channel used for the application
    -- Using topK to get the most frequent channel
    arrayElement(topK(1)(channel), 1) AS application_channel,

    -- Submission time metrics (only for completed applications)
    MIN(CASE WHEN status='completed' THEN time_to_submit_sec END) AS min_time_to_submit_sec,
    AVG(CASE WHEN status='completed' THEN time_to_submit_sec END) AS avg_time_to_submit_sec

FROM upload_applications_20260220015647
GROUP BY session_id;