-- stg_users.sql
-- Purpose: Clean and type-cast the users source table.
-- One row per user.

SELECT
    user_id,
    -- Standardise region to lowercase for consistent grouping
    LOWER(region) AS region,

    -- Cast string flags to booleans for easier filtering and aggregation
    (disability_flag = 'yes') AS is_disabled,
    (first_time_digital_user = 'yes') AS is_first_time_user,

    -- Derived flag: user belongs to a vulnerable segment
    -- This is used as a shorthand in KPI queries downstream
    (disability_flag = 'yes'
     OR first_time_digital_user = 'yes') AS is_vulnerable_user

FROM upload_users_20260220015436
