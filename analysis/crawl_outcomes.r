library(duckdb)
library(ggplot2)

db_conn <- dbConnect(duckdb())

total_seeds <- 25000

# Materialize depth-0 responses
dbExecute(db_conn, "
  CREATE TEMP TABLE d0 AS
  SELECT experiment_id, crawled_url, url, status
  FROM read_parquet('../results/*/enriched/responses.parquet')
  WHERE crawl_depth = 0
")

# Classify each unique site by its best outcome across all experiments.
# A site counts as "Loaded" if:
#   - The main navigation returned 2xx, OR
#   - It redirected (3xx) and at least one subresource returned 200, OR
#   - It returned 403 on the bare URL but loaded content anyway (>5 successful subresources)
df <- dbGetQuery(db_conn, "
  WITH per_site AS (
    SELECT
      crawled_url,
      MAX(CASE WHEN url = crawled_url AND status BETWEEN 200 AND 299 THEN 1 ELSE 0 END) AS has_direct_2xx,
      MAX(CASE WHEN url = crawled_url AND status BETWEEN 300 AND 399 THEN 1 ELSE 0 END) AS has_redirect,
      MAX(CASE WHEN url = crawled_url AND status = 403 THEN 1 ELSE 0 END) AS has_403,
      MAX(CASE WHEN url = crawled_url AND status BETWEEN 400 AND 499 AND status != 403 THEN 1 ELSE 0 END) AS has_4xx,
      MAX(CASE WHEN url = crawled_url AND status BETWEEN 500 AND 599 THEN 1 ELSE 0 END) AS has_5xx,
      COUNT(CASE WHEN status = 200 THEN 1 END) AS total_200s
    FROM d0
    GROUP BY crawled_url
  )
  SELECT
    CASE
      WHEN has_direct_2xx = 1 THEN 'Loaded'
      WHEN has_redirect = 1 AND total_200s > 0 THEN 'Loaded'
      WHEN has_redirect = 1 THEN 'Redirect (no content)'
      WHEN has_403 = 1 AND total_200s > 5 THEN 'Loaded'
      WHEN has_403 = 1 THEN 'Blocked (403)'
      WHEN has_4xx = 1 THEN 'Client Error (4xx)'
      WHEN has_5xx = 1 THEN 'Server Error (5xx)'
      ELSE 'Other'
    END AS category,
    COUNT(*) AS sites
  FROM per_site
  GROUP BY category
")

# Per-experiment breakdown
by_exp <- dbGetQuery(db_conn, "
  WITH per_site_exp AS (
    SELECT
      experiment_id,
      crawled_url,
      MAX(CASE WHEN url = crawled_url AND status BETWEEN 200 AND 299 THEN 1 ELSE 0 END) AS has_direct_2xx,
      MAX(CASE WHEN url = crawled_url AND status BETWEEN 300 AND 399 THEN 1 ELSE 0 END) AS has_redirect,
      MAX(CASE WHEN url = crawled_url AND status = 403 THEN 1 ELSE 0 END) AS has_403,
      MAX(CASE WHEN url = crawled_url AND status BETWEEN 400 AND 499 AND status != 403 THEN 1 ELSE 0 END) AS has_4xx,
      MAX(CASE WHEN url = crawled_url AND status BETWEEN 500 AND 599 THEN 1 ELSE 0 END) AS has_5xx,
      COUNT(CASE WHEN status = 200 THEN 1 END) AS total_200s
    FROM d0
    GROUP BY experiment_id, crawled_url
  )
  SELECT
    experiment_id,
    CASE
      WHEN has_direct_2xx = 1 THEN 'Loaded'
      WHEN has_redirect = 1 AND total_200s > 0 THEN 'Loaded'
      WHEN has_redirect = 1 THEN 'Redirect (no content)'
      WHEN has_403 = 1 AND total_200s > 5 THEN 'Loaded'
      WHEN has_403 = 1 THEN 'Blocked (403)'
      WHEN has_4xx = 1 THEN 'Client Error (4xx)'
      WHEN has_5xx = 1 THEN 'Server Error (5xx)'
      ELSE 'Other'
    END AS category,
    COUNT(*) AS sites
  FROM per_site_exp
  GROUP BY experiment_id, category
  ORDER BY experiment_id, sites DESC
")

dbDisconnect(db_conn)

# Add "No Response" for seeds that never got any response
responded <- sum(df$sites)
df <- rbind(df, data.frame(category = "No Response", sites = total_seeds - responded))

df$pct <- df$sites / total_seeds * 100

# --- Terminal output ---
experiments <- unique(by_exp$experiment_id)
for (exp in experiments) {
  sub <- by_exp[by_exp$experiment_id == exp, ]
  exp_responded <- sum(sub$sites)
  no_resp <- total_seeds - exp_responded
  sub <- rbind(sub, data.frame(experiment_id = exp, category = "No Response", sites = no_resp))
  cat(sprintf('\n=== %s ===\n\n', exp))
  for (i in seq_len(nrow(sub))) {
    pct <- sub$sites[i] / total_seeds * 100
    cat(sprintf('  %-25s %5d  (%4.1f%%)\n', sub$category[i], sub$sites[i], pct))
  }
}

cat(sprintf('\n=== Aggregated (best outcome per site, %d seeded) ===\n\n', total_seeds))
for (i in seq_len(nrow(df))) {
  cat(sprintf('  %-25s %5d  (%4.1f%%)\n', df$category[i], df$sites[i], df$pct[i]))
}

# --- Donut chart ---

# 1. Remove empty categories first so they don't affect the sorting
df <- df[df$sites > 0, ]

# 2. Dynamically order categories by percentage (highest to lowest)
level_order <- df$category[order(df$pct, decreasing = TRUE)]

# 3. Apply the factor levels (reversed for correct ggplot polar stacking)
df$category <- factor(df$category, levels = rev(level_order))

# Create the multi-line label string for the slices on the chart
df$chart_label <- paste0(as.character(df$category), "\n",
                         format(df$sites, big.mark = ","), " sites (", round(df$pct, 1), "%)")

# Filter chart labels: Only keep text for categories that make up more than 5% of the total
df$chart_label <- ifelse(df$pct >= 5, df$chart_label, "")

# Create legend labels
df$legend_label <- paste0(as.character(df$category), "\n",
                          format(df$sites, big.mark = ","), " sites (", round(df$pct, 1), "%)")

legend_mapping <- setNames(df$legend_label, as.character(df$category))

colors <- c(
  "Loaded"                = "#22c55e",
  "Blocked (403)"         = "#ef4444",
  "Client Error (4xx)"    = "#f97316",
  "Server Error (5xx)"    = "#a855f7",
  "Redirect (no content)" = "#eab308",
  "No Response"           = "#94a3b8",
  "Other"                 = "#d1d5db"
)

ggplot(df, aes(x = 2, y = sites, fill = category)) +
  geom_col(width = 1, color = "white", linewidth = 0.5) +
  coord_polar(theta = "y") +
  xlim(0.5, 2.5) +
  geom_text(
    aes(label = chart_label),
    position = position_stack(vjust = 0.5),
    size = 3.5, color = "white", fontface = "bold",
    na.rm = TRUE 
  ) +
scale_fill_manual(
  values = colors,
  breaks = level_order,
  labels = legend_mapping[level_order]
) +
guides(fill = guide_legend(
  byrow = TRUE,                     # Required for legend.spacing.y to work properly
  keyheight = unit(2.2, "lines"),   # Slightly taller row keys
  keywidth  = unit(1.2, "lines"),
  label.theme = element_text(size = 10, lineheight = 1.3)
)) +
  labs(fill = NULL) +
  theme_void(base_size = 13) +
  theme(
    legend.spacing.y = unit(0.4, "cm") # Controls the actual padding between legend items
  )

ggsave("plots/crawl_outcomes.pdf", width = 8, height = 6)
