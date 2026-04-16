library(duckdb)
library(ggplot2)

db_conn <- dbConnect(duckdb())

df_worker_dist <- dbGetQuery(db_conn, "
  WITH sites_per_worker AS (
    SELECT
      worker_id,
      COUNT(DISTINCT crawl_session_id) AS sites_crawled
    FROM read_parquet('../results/*/enriched/labeled_requests.parquet', filename=true)
    GROUP BY 1
  ),
  worker_countries AS (
    SELECT DISTINCT ON (worker_id)
      worker_id,
      country_code
    FROM read_parquet('../results/*/enriched/worker_metadata.parquet', filename=true)
    ORDER BY worker_id, timestamp DESC
  )
  SELECT
    COALESCE(w.country_code, 'Unknown') AS country_code,
    SUM(s.sites_crawled) AS sites_crawled
  FROM sites_per_worker s
  LEFT JOIN worker_countries w ON s.worker_id = w.worker_id
  GROUP BY 1
  ORDER BY sites_crawled DESC
")

dbDisconnect(db_conn)

# Keep top 10 countries
df_worker_dist <- head(df_worker_dist, 10)
df_worker_dist$country_code <- factor(df_worker_dist$country_code, levels = df_worker_dist$country_code)

ggplot(df_worker_dist, aes(x = country_code, y = sites_crawled)) +
  geom_col(fill = "#6366f1", width = 0.6) +
  scale_y_continuous(labels = scales::label_comma(), breaks = scales::breaks_pretty(n = 8)) +
  labs(
    x = "Country",
    y = "Sites Crawled"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

ggsave("plots/worker_distribution.pdf", width = 7, height = 5)
