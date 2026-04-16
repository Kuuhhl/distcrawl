-- @materialize: true
WITH ranked_requests AS (
    SELECT
        exp_id AS experiment_id,
        crawl_session_id,
        to_timestamp(CAST(timestamp AS DOUBLE)) AT TIME ZONE 'Europe/Berlin' AS scraped_at,
        -- Rank each row within its specific session by time
        ROW_NUMBER() OVER (
            PARTITION BY crawl_session_id 
            ORDER BY timestamp ASC
        ) AS session_row_num
    FROM requests
)
SELECT
    experiment_id,
    scraped_at,
    -- Re-calculating completed sites based only on these first-session-requests
    ROW_NUMBER() OVER (PARTITION BY experiment_id ORDER BY scraped_at) AS sites_completed
FROM ranked_requests
WHERE session_row_num = 1