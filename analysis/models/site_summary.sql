-- @materialize: true
SELECT
  meta.exp_id AS experiment_id,
  meta.params.navigate_subpages AS experiment_navigate_subpages,
  meta.params.auto_accept_cookies AS experiment_auto_accept_cookies,
  meta.params.browser_type AS experiment_browser_type,
  meta.params.headless AS experiment_headless,

  wm.country_code AS worker_country_code,
  wm.is_residential AS worker_is_residential,

  res.status AS response_status,
  req.resource_type AS request_resource_type,

  req.crawled_url AS request_crawled_url,
  regexp_extract(req.crawled_url, '^(?:https?://)?(?:[^@/\n]+@)?(?:www\.)?([^:/ \n]+)', 1) AS seed_domain,

  regexp_extract(req.url, '^(?:https?://)?(?:[^@/\n]+@)?(?:www\.)?([^:/ \n]+)', 1) AS request_requested_domain,

  req.crawl_depth AS request_crawl_depth,
  CASE WHEN req.crawl_depth = 0 THEN 'Homepage' ELSE 'Subpage' END AS request_page_type,
  req.blocked_by AS request_blocked_by,
  CASE WHEN req.blocked_by != '[]' THEN TRUE ELSE FALSE END AS request_is_tracker,

  CASE WHEN ck.crawl_session_id IS NOT NULL THEN TRUE ELSE FALSE END AS worker_clicked_banner

FROM metadata meta
LEFT JOIN requests req ON meta.exp_id = req.exp_id
LEFT JOIN worker_metadata wm ON req.worker_id = wm.worker_id
LEFT JOIN responses res ON req.request_id = res.request_id

LEFT JOIN site_metadata sm
    ON req.crawl_session_id = sm.crawl_session_id

LEFT JOIN cookies ck
    ON req.crawl_session_id = ck.crawl_session_id
