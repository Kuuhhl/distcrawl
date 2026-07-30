#!/usr/bin/env python3
"""Build results page checking per-site crawl success via site_metadata + requests join."""

import os
import sys
from datetime import datetime

import duckdb
from jinja2 import Template

EXPECTED_SITES = ["wikipedia.com", "duckduckgo.com", "landmann.ph"]


def _parse_ts(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        try:
            return datetime.fromtimestamp(float(ts))
        except (ValueError, TypeError):
            return None


def _format_ts(ts: str) -> str:
    dt = _parse_ts(ts)
    if dt is None:
        return ts

    day = dt.day
    if 4 <= day <= 20 or 24 <= day <= 30:
        suffix = "th"
    else:
        suffix = ["st", "nd", "rd"][day % 10 - 1]
    return dt.strftime(f"%-d{suffix} of %B, at %-I:%M%p")


def _site_data(
    con: duckdb.DuckDBPyConnection, bucket: str, url: str
) -> dict | None:
    row = con.execute(f"""
        SELECT
            sm.description,
            sm.timestamp,
            wm.country_code,
            COALESCE(reqs.cnt, 0) AS requests,
            COALESCE(resps.cnt, 0) AS responses
        FROM (
            SELECT DISTINCT crawl_session_id, worker_id
            FROM read_parquet('s3://{bucket}/experiment=*/data_type=requests/*.parquet', hive_partitioning=true)
            WHERE crawled_url LIKE '%{url}%'
        ) r
        LEFT JOIN (
            SELECT DISTINCT crawl_session_id, description, timestamp
            FROM read_parquet('s3://{bucket}/experiment=*/data_type=site_metadata/*.parquet', hive_partitioning=true)
        ) sm ON sm.crawl_session_id = r.crawl_session_id
        LEFT JOIN (
            SELECT DISTINCT worker_id, country_code
            FROM read_parquet('s3://{bucket}/experiment=*/data_type=worker_metadata/*.parquet', hive_partitioning=true)
        ) wm ON wm.worker_id = r.worker_id
        LEFT JOIN (
            SELECT COUNT(*) AS cnt
            FROM read_parquet('s3://{bucket}/experiment=*/data_type=requests/*.parquet', hive_partitioning=true)
            WHERE crawled_url LIKE '%{url}%'
        ) reqs ON true
        LEFT JOIN (
            SELECT COUNT(*) AS cnt
            FROM read_parquet('s3://{bucket}/experiment=*/data_type=responses/*.parquet', hive_partitioning=true)
            WHERE crawled_url LIKE '%{url}%'
        ) resps ON true
        LIMIT 1
    """).fetchone()

    if row is None:
        return None

    return {
        "description": row[0] or "",
        "timestamp": row[1] or "",
        "country_code": row[2] or "",
        "requests": row[3],
        "responses": row[4],
    }


def _crawl_duration(con: duckdb.DuckDBPyConnection, bucket: str) -> str:
    metadata_glob = (
        f"s3://{bucket}/experiment=*/data_type=site_metadata/*.parquet"
    )
    has_metadata = con.execute(
        "SELECT 1 FROM glob(?) LIMIT 1", [metadata_glob]
    ).fetchone()
    if has_metadata is None:
        return ""

    row = con.execute(f"""
        SELECT MIN(timestamp), MAX(timestamp)
        FROM read_parquet('{metadata_glob}', hive_partitioning=true)
    """).fetchone()
    if not row or not row[0] or not row[1]:
        return ""

    t1 = _parse_ts(row[0])
    t2 = _parse_ts(row[1])
    if t1 is None or t2 is None:
        return ""

    delta = t2 - t1
    total_seconds = int(delta.total_seconds())
    if total_seconds < 60:
        return f"{total_seconds}s"
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}m {seconds}s"


def main():
    bucket = os.environ.get("RESULTS_BUCKET_NAME")
    cloudflare_account_id = os.environ.get("TF_VAR_cloudflare_account_id")
    key = os.environ.get("S3_ACCESS_KEY")
    secret = os.environ.get("S3_SECRET_KEY")

    if not all([bucket, cloudflare_account_id, key, secret]):
        print("Missing env vars", file=sys.stderr)
        sys.exit(1)

    endpoint = f"{cloudflare_account_id}.r2.cloudflarestorage.com"

    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{key}',
            SECRET '{secret}',
            ENDPOINT '{endpoint}',
            REGION 'auto',
            URL_STYLE 'path'
        );
    """)

    crawl_duration = _crawl_duration(con, bucket)

    sites = []
    for url in EXPECTED_SITES:
        site = {
            "url": url,
            "meta_desc": "",
            "status": "could not be crawled",
            "requests": 0,
            "responses": 0,
            "crawler_country": "",
            "crawl_time": "",
        }
        try:
            data = _site_data(con, bucket, url)
            if data and data["timestamp"]:
                site["status"] = "crawled"
                site["meta_desc"] = data["description"]
                site["requests"] = data["requests"]
                site["responses"] = data["responses"]
                site["crawler_country"] = data["country_code"]
                site["crawl_time"] = (
                    _format_ts(data["timestamp"]) if data["timestamp"] else ""
                )
        except Exception as e:
            print(f"Warning: failed to query {url}: {e}", file=sys.stderr)
        sites.append(site)

    with open("demo/website_template/index.html") as f:
        template = Template(f.read(), autoescape=True)
    html = template.render(
        date=datetime.now().strftime("%Y-%m-%d"),
        crawl_duration=crawl_duration,
        sites=sites,
    )

    os.makedirs("public", exist_ok=True)
    with open("public/index.html", "w") as f:
        f.write(html)

    print("Generated public/index.html")


if __name__ == "__main__":
    main()
