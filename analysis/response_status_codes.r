library(duckdb)
library(ggplot2)
library(tidyr)

db_conn <- dbConnect(duckdb())

df_experiment <- dbGetQuery(db_conn, "
  WITH requests AS (
    SELECT
      regexp_extract(filename, 'results/([^/]+)/', 1) AS experiment_id,
      COUNT(*) AS requests,
      COUNT(*) FILTER (WHERE blocked_by IS NOT NULL AND blocked_by != '[]') AS trackers
    FROM read_parquet('../results/*/enriched/labeled_requests.parquet', filename=true)
    GROUP BY 1
  ),
  responses AS (
    SELECT
      regexp_extract(filename, 'results/([^/]+)/', 1) AS experiment_id,
      COUNT(*) AS responses
    FROM read_parquet('../results/*/enriched/responses.parquet', filename=true)
    GROUP BY 1
  )
  SELECT
    r.experiment_id,
    r.requests,
    resp.responses,
    r.trackers
  FROM requests r
  JOIN responses resp ON r.experiment_id = resp.experiment_id
")

dbDisconnect(db_conn)

df_experiment$experiment_id <- factor(df_experiment$experiment_id,
  levels = c(
    "chromium25kohnecookies_bea6cc6a",
    "firefox25kmitcookies_4b84a683",
    "chromium25kmitcookies_d010ca7c"
  ),
  labels = c(
    "Exp. 1\n(Chromium w/o cookies)",
    "Exp. 2\n(Firefox w/ cookies)",
    "Exp. 3\n(Chromium w/ cookies)"
  )
)

df_long <- pivot_longer(df_experiment,
  cols = c(requests, responses, trackers),
  names_to = "metric", values_to = "count"
)
df_long$metric <- factor(df_long$metric,
  levels = c("requests", "responses", "trackers"),
  labels = c("Requests", "Responses", "Trackers")
)

ggplot(df_long, aes(x = metric, y = count, fill = metric)) +
  geom_col(width = 0.6) +
  facet_wrap(~experiment_id, nrow = 1) +
  scale_fill_manual(values = c(
    "Requests"  = "#6366f1",
    "Responses" = "#06b6d4",
    "Trackers"  = "#f43f5e"
  )) +
  scale_y_continuous(labels = scales::label_comma()) +
  labs(
    x = NULL,
    y = "Count"
  ) +
  guides(fill = "none") +
  theme_minimal(base_size = 14) +
  theme(
    panel.grid.minor = element_blank(),
    strip.text = element_text(size = 11)
  )

ggsave("plots/experiment_overview.pdf", width = 9, height = 5)
