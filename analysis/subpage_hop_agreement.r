library(duckdb)

db_conn <- dbConnect(duckdb())

# For each crawl_depth level, find what % of subpage hops (crawled_url at that depth)
# are visited identically by all 3 browsers/experiments.
# Only includes sites that successfully completed in all 3 experiments (present in site_metadata).

df <- dbGetQuery(db_conn, "
  WITH experiments AS (
    SELECT DISTINCT experiment_id
    FROM read_parquet('../results/*/enriched/labeled_requests.parquet', filename=true)
  ),
  num_experiments AS (
    SELECT COUNT(*) AS n FROM experiments
  ),
  -- crawl_session_ids that have a site_metadata entry (= successful crawl)
  successful_sessions AS (
    SELECT DISTINCT
      regexp_extract(filename, 'results/([^/]+)/', 1) AS experiment_id,
      crawl_session_id
    FROM read_parquet('../results/*/enriched/site_metadata.parquet', hive_partitioning=0, filename=true)
  ),
  -- map each successful session to its root URL (crawled_url at depth 0)
  session_roots AS (
    SELECT DISTINCT r.experiment_id, r.crawl_session_id, r.crawled_url AS root_url
    FROM read_parquet('../results/*/enriched/labeled_requests.parquet', filename=true) r
    JOIN successful_sessions s
      ON r.experiment_id = s.experiment_id AND r.crawl_session_id = s.crawl_session_id
    WHERE r.crawl_depth = 0 AND r.resource_type = 'document'
  ),
  -- root URLs successfully crawled by all 3 experiments
  shared_roots AS (
    SELECT root_url
    FROM session_roots
    GROUP BY root_url
    HAVING COUNT(DISTINCT experiment_id) = (SELECT n FROM num_experiments)
  ),
  doc_hops AS (
    SELECT DISTINCT
      r.experiment_id,
      r.crawl_depth,
      r.crawled_url
    FROM read_parquet('../results/*/enriched/labeled_requests.parquet', filename=true) r
    JOIN session_roots sr
      ON r.experiment_id = sr.experiment_id AND r.crawl_session_id = sr.crawl_session_id
    WHERE r.resource_type = 'document'
      AND sr.root_url IN (SELECT root_url FROM shared_roots)
  ),
  hop_experiment_counts AS (
    SELECT
      crawl_depth,
      crawled_url,
      COUNT(DISTINCT experiment_id) AS experiments_with_hop
    FROM doc_hops
    GROUP BY crawl_depth, crawled_url
  ),
  level_stats AS (
    SELECT
      crawl_depth,
      COUNT(*) AS total_hops,
      SUM(CASE WHEN experiments_with_hop = (SELECT n FROM num_experiments) THEN 1 ELSE 0 END) AS shared_hops
    FROM hop_experiment_counts
    GROUP BY crawl_depth
  )
  SELECT
    crawl_depth,
    total_hops,
    shared_hops,
    ROUND(100.0 * shared_hops / total_hops, 1) AS pct_shared
  FROM level_stats
  ORDER BY crawl_depth
")

dbDisconnect(db_conn)

cat(sprintf("%-10s  %12s  %12s  %10s\n", "Level", "Total Hops", "Shared Hops", "% Shared"))
cat(strrep("-", 50), "\n")
for (i in seq_len(nrow(df))) {
  cat(sprintf("%-10s  %12s  %12s  %9s%%\n",
    paste("Level", df$crawl_depth[i]),
    format(df$total_hops[i], big.mark = ","),
    format(df$shared_hops[i], big.mark = ","),
    df$pct_shared[i]
  ))
}
