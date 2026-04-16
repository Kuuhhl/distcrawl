library(duckdb)
library(ggplot2)

db_conn <- dbConnect(duckdb())

df <- dbGetQuery(db_conn, "
  WITH total_sites AS (
    SELECT COUNT(DISTINCT crawled_url) AS total
    FROM read_parquet('../results/*/enriched/labeled_requests.parquet')
  ),
  cookie_sites AS (
    SELECT COUNT(DISTINCT crawled_url) AS with_cookie
    FROM read_parquet('../results/*/enriched/cookie_warning_consents.parquet')
  )
  SELECT
    cookie_sites.with_cookie,
    total_sites.total - cookie_sites.with_cookie AS without_cookie
  FROM total_sites, cookie_sites
")

dbDisconnect(db_conn)

df_pie <- data.frame(
  category = c("With Cookie Warning", "Without Cookie Warning"),
  count = c(df$with_cookie, df$without_cookie)
)
df_pie$pct <- df_pie$count / sum(df_pie$count) * 100
df_pie$label <- paste0("~", round(df_pie$pct, 1), "%")

ggplot(df_pie, aes(x = "", y = count, fill = category)) +
  geom_col(width = 1) +
  coord_polar(theta = "y") +
  scale_fill_manual(values = c(
    "With Cookie Warning"    = "#f59e0b",
    "Without Cookie Warning" = "#6366f1"
  )) +
  geom_text(
    aes(label = label),
    position = position_stack(vjust = 0.5),
    size = 4.5, color = "white", fontface = "bold"
  ) +
  labs(fill = NULL) +
  theme_void(base_size = 14)

ggsave("plots/cookie_warnings.pdf", width = 7, height = 6)
