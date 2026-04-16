library(duckdb)

db_conn <- dbConnect(duckdb())

# Materialize crawl_depth=0 responses for performance
dbExecute(db_conn, "
  CREATE TEMP TABLE main_doc AS
  SELECT experiment_id, crawled_url, url, status
  FROM read_parquet('../results/*/enriched/responses.parquet')
  WHERE crawl_depth = 0
")

# A site is considered blocked if:
#   1. The initial navigation (url = crawled_url) returned HTTP 403
#   2. The page did not actually load (at most 5 successful subresources)
#      This filters out false positives where a bare domain returns 403
#      but the browser follows a redirect and the page loads fine.

# --- Per browser ---
by_browser <- dbGetQuery(db_conn, "
  WITH blocked AS (
    SELECT experiment_id, crawled_url
    FROM main_doc
    WHERE url = crawled_url AND status = 403
  ),
  filtered AS (
    SELECT b.experiment_id, b.crawled_url
    FROM blocked b
    WHERE (
      SELECT COUNT(*)
      FROM main_doc s
      WHERE s.crawled_url = b.crawled_url
        AND s.experiment_id = b.experiment_id
        AND s.status = 200
    ) <= 5
  )
  SELECT
    browser,
    total,
    blocked
  FROM (
    SELECT
      CASE
        WHEN t.experiment_id ILIKE '%firefox%' THEN 'Firefox'
        ELSE 'Chromium'
      END AS browser,
      COUNT(DISTINCT t.crawled_url) AS total
    FROM main_doc t
    WHERE t.url = t.crawled_url
    GROUP BY browser
  ) totals
  JOIN (
    SELECT
      CASE
        WHEN f.experiment_id ILIKE '%firefox%' THEN 'Firefox'
        ELSE 'Chromium'
      END AS browser,
      COUNT(DISTINCT f.crawled_url) AS blocked
    FROM filtered f
    GROUP BY browser
  ) blocks USING (browser)
  ORDER BY browser
")

cat('=== Bot detection blocks (HTTP 403 on main document) per browser ===\n\n')
for (i in seq_len(nrow(by_browser))) {
  pct <- by_browser$blocked[i] / by_browser$total[i] * 100
  cat(sprintf('  %-10s  %d / %d sites blocked  (%.1f%%)\n',
              by_browser$browser[i], by_browser$blocked[i], by_browser$total[i], pct))
}

# --- Overall (unique sites across all experiments) ---
overall <- dbGetQuery(db_conn, "
  WITH blocked AS (
    SELECT experiment_id, crawled_url
    FROM main_doc
    WHERE url = crawled_url AND status = 403
  ),
  filtered AS (
    SELECT experiment_id, crawled_url
    FROM blocked
    WHERE (
      SELECT COUNT(*)
      FROM main_doc s
      WHERE s.crawled_url = blocked.crawled_url
        AND s.experiment_id = blocked.experiment_id
        AND s.status = 200
    ) <= 5
  )
  SELECT
    (SELECT COUNT(DISTINCT crawled_url) FROM main_doc WHERE url = crawled_url) AS total,
    COUNT(DISTINCT crawled_url) AS blocked
  FROM filtered
")

cat(sprintf('\n=== Overall (unique sites) ===\n\n'))
cat(sprintf('  %d / %d sites blocked  (%.1f%%)\n',
            overall$blocked, overall$total, overall$blocked / overall$total * 100))

dbDisconnect(db_conn)
