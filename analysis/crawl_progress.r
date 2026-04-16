library(duckdb)
library(ggplot2)

db_conn <- dbConnect(duckdb())
dbExecute(db_conn, "INSTALL icu; LOAD icu;")

df_crawl_progress <- dbGetQuery(db_conn, "
  WITH ranked_requests AS (
    SELECT
      regexp_extract(filename, 'results/([^/]+)/', 1) AS experiment_id,
      crawl_session_id,
      to_timestamp(CAST(timestamp AS DOUBLE)) AT TIME ZONE 'Europe/Berlin' AS scraped_at,
      ROW_NUMBER() OVER (
        PARTITION BY crawl_session_id
        ORDER BY timestamp ASC
      ) AS session_row_num
    FROM read_parquet('../results/*/enriched/labeled_requests.parquet', filename=true)
  )
  SELECT
    experiment_id,
    scraped_at,
    ROW_NUMBER() OVER (PARTITION BY experiment_id ORDER BY scraped_at) AS sites_completed
  FROM ranked_requests
  WHERE session_row_num = 1
")

dbDisconnect(db_conn)

# Reassigned levels and labels to match the table
df_crawl_progress$experiment_id <- factor(df_crawl_progress$experiment_id,
  levels = c(
    "chromium25kohnecookies_bea6cc6a",
    "firefox25kmitcookies_4b84a683",
    "chromium25kmitcookies_d010ca7c"
  ),
  labels = c(
    "Experiment 1 (Chromium w/o cookies)",
    "Experiment 2 (Firefox w/ cookies)",
    "Experiment 3 (Chromium w/ cookies)"
  )
)

start_time <- min(df_crawl_progress$scraped_at)
df_crawl_progress$hours_elapsed <- as.numeric(difftime(df_crawl_progress$scraped_at, start_time, units = "hours"))

# Updated colors to match the new naming structure
palette_experiment_colors <- c(
  "Experiment 1 (Chromium w/o cookies)" = "#8b5cf6",
  "Experiment 2 (Firefox w/ cookies)"   = "#ec4899",
  "Experiment 3 (Chromium w/ cookies)"  = "#10b981"
)

time_node_reassignment <- as.POSIXct("2026-04-05 13:38:00", tz = "UTC")
hours_node_reassignment <- as.numeric(difftime(time_node_reassignment, start_time, units = "hours"))

# Shifted logic to target Experiment 3 for the trend line
df_exp3 <- df_crawl_progress[df_crawl_progress$experiment_id == "Experiment 3 (Chromium w/ cookies)", ]
df_exp3_pre_reassignment <- df_exp3[df_exp3$hours_elapsed <= hours_node_reassignment, ]

model_exp3_trend <- lm(sites_completed ~ hours_elapsed, data = df_exp3_pre_reassignment)
df_trend_projection <- data.frame(hours_elapsed = c(min(df_exp3$hours_elapsed), max(df_exp3$hours_elapsed)))
df_trend_projection$sites_completed <- predict(model_exp3_trend, newdata = df_trend_projection)

proj_label_x <- max(df_trend_projection$hours_elapsed)
proj_label_y <- df_trend_projection$sites_completed[2]

proj_text_x <- proj_label_x + 1
proj_text_y <- proj_label_y - 10000

max_hours <- ceiling(max(df_crawl_progress$hours_elapsed))
hour_breaks <- seq(0, max_hours, by = 1)
hour_labels <- paste0(hour_breaks, "h")

ggplot(df_crawl_progress, aes(x = hours_elapsed, y = sites_completed, color = experiment_id)) +
  geom_line(linewidth = 0.6) +
  geom_line(
    data = df_trend_projection, aes(x = hours_elapsed, y = sites_completed),
    inherit.aes = FALSE, linetype = "dashed", color = "#10b981", linewidth = 0.6, alpha = 0.8
  ) +
  annotate("text",
    x = proj_text_x, y = proj_text_y,
    label = "Projection based on\n50 nodes",
    hjust = 1, vjust = 0.5, size = 4, color = "#10b981", fontface = "italic"
  ) +
  annotate("segment",
    x = proj_text_x - 0.7, y = proj_text_y + 2000,
    xend = proj_label_x - 0.2, yend = proj_label_y - 2000,
    arrow = arrow(length = unit(0.15, "cm"), type = "closed"), color = "#10b981"
  ) +
  geom_vline(xintercept = hours_node_reassignment, linetype = "dotted", color = "grey40") +
  geom_hline(yintercept = 25000, linetype = "longdash", color = "grey40") +
  annotate("text",
    x = 0, y = 25000, label = "25k target",
    hjust = -0.05, vjust = -0.5, size = 4, color = "grey30"
  ) +
  annotate("text",
    x = hours_node_reassignment, y = Inf, label = "Node reassignment\n(50 -> 100 for Chromium)",
    vjust = 5, hjust = 1.1, size = 4, color = "grey30"
  ) +
  annotate("segment",
    x = hours_node_reassignment - 0.8, y = 17000,
    xend = hours_node_reassignment - 0.2, yend = 20000,
    arrow = arrow(length = unit(0.15, "cm"), type = "closed"), color = "grey40"
  ) +
  scale_x_continuous(breaks = hour_breaks, labels = hour_labels) +
  scale_color_manual(values = palette_experiment_colors) +
  scale_y_continuous(labels = scales::label_comma()) +
  labs(
    x = "Hours since start",
    y = "Sites Crawled",
    color = ""
  ) +
  guides(color = guide_legend(override.aes = list(linewidth = 3))) +
  theme_minimal(base_size = 14) +
  theme(
    legend.position = "bottom",
    panel.grid.minor = element_blank()
  )

ggsave("plots/crawl_progress.pdf", width = 10, height = 5)
