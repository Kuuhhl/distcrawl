-- @materialize: true
WITH base AS (
  SELECT
    *,
    lower(split_part(seed_domain, '.', -1)) AS website_tld,
    CAST(request_crawl_depth AS INTEGER) AS request_crawl_depth_int,
    split_part(request_crawled_url, '?', 1) AS request_base_url
  FROM site_summary
),

signatures AS (
  SELECT
    request_base_url AS sig_url,
    request_requested_domain AS sig_domain,

    CASE
      WHEN bool_or(experiment_auto_accept_cookies = TRUE)
       AND bool_or(experiment_auto_accept_cookies = FALSE)
       THEN 'regardless (both)'
      WHEN bool_or(experiment_auto_accept_cookies = TRUE)
       AND NOT bool_or(experiment_auto_accept_cookies = FALSE)
       THEN 'only when cookies accepted'
      WHEN NOT bool_or(experiment_auto_accept_cookies = TRUE)
       AND bool_or(experiment_auto_accept_cookies = FALSE)
       THEN 'only when cookies not accepted'
      ELSE 'unclassified'
    END AS cookie_request_class,

    min(request_crawl_depth_int) AS min_depth,

    array_to_string(
      list_sort(list_distinct(list(experiment_browser_type))),
      ' + '
    ) AS browser_signature

  FROM base
  GROUP BY request_base_url, request_requested_domain
)

SELECT
  b.*,

  tr.tranco_rank AS seed_website_rank,
  CASE
    WHEN tr.tranco_rank <= 1000 THEN '1. Top 1K'
    WHEN tr.tranco_rank <= 10000 THEN '2. 1K - 10K'
    WHEN tr.tranco_rank <= 50000 THEN '3. 10K - 50K'
    WHEN tr.tranco_rank IS NOT NULL THEN '4. 50K+'
    ELSE '5. Unknown/Unranked'
  END AS seed_rank_bucket,

  COALESCE(r.continent, 'Global/Generic') AS website_continent,
  CASE
    WHEN r.is_eu THEN 'EU (GDPR)'
    WHEN b.website_tld = 'uk' THEN 'UK (Post-Brexit GDPR)'
    WHEN b.website_tld = 'ch' THEN 'Switzerland (DPA)'
    WHEN length(b.website_tld) = 2 AND r.continent IS NOT NULL THEN 'Non-EU ccTLD'
    ELSE 'Global/Generic'
  END AS website_legal_zone,

  s.cookie_request_class,
  s.min_depth,
  s.browser_signature

FROM base b
LEFT JOIN tranco tr ON b.seed_domain = tr.domain
LEFT JOIN region_catalog r ON b.website_tld = r.tld_key
LEFT JOIN signatures s ON b.request_base_url = s.sig_url AND b.request_requested_domain = s.sig_domain
