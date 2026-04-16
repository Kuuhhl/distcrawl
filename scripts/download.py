import asyncio
import json
import logging
import os
import shutil
import sys
import tempfile
from typing import Any, List, Optional

import adblock
import aiohttp
import defopt
import duckdb
import obstore as obs
from dist_common import ExperimentMetadata
from config import ScriptSettings

settings = ScriptSettings()
logging.basicConfig(
    level=settings.logging_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


DEFAULT_FILTER_LISTS: List[str] = [
    "https://easylist.to/easylist/easylist.txt",
    "https://easylist.to/easylist/easyprivacy.txt",
    "https://easylist.to/easylistgermany/easylistgermany.txt",
    "https://stanev.org/abp/adblock_bg.txt",
    "https://easylist-downloads.adblockplus.org/easylistdutch.txt",
    "https://raw.githubusercontent.com/EasyList-Lithuania/easylist_lithuania/master/easylistlithuania.txt",
    "https://easylist-downloads.adblockplus.org/easylistitaly.txt",
    "https://easylist-downloads.adblockplus.org/easylistpolish.txt",
    "https://easylist-downloads.adblockplus.org/easylistportuguese.txt",
    "https://easylist-downloads.adblockplus.org/ruadlist.txt",
    "https://zoso.ro/pages/rolist.txt",
    "https://easylist-downloads.adblockplus.org/liste_fr.txt",
]


def download_metadata(results_dir: str) -> Optional[ExperimentMetadata]:
    """Load metadata.json from the results directory."""
    metadata_path = os.path.join(results_dir, "metadata.json")
    if not os.path.exists(metadata_path):
        logger.warning("No metadata.json found in %s", results_dir)
        return None
    try:
        with open(metadata_path, "r") as f:
            data = json.load(f)
        return ExperimentMetadata(**data)
    except Exception as exc:
        logger.error("Metadata loading failed: %s", exc)
        return None


async def sync_results_from_s3(local_dir: str, config: ScriptSettings) -> bool:
    """
    Download all objects from the results bucket to a local directory.

    :param local_dir: Local directory to download to
    :param config: ScriptSettings with S3 credentials
    :return: True if download succeeded, False otherwise
    """
    try:
        store = config.get_storage()
        logger.info("Downloading objects from bucket: %s", config.results_bucket)

        all_objects = await obs.list(store).collect_async()
        logger.info("Found %d objects to download", len(all_objects))

        from tqdm import tqdm

        total_bytes = sum(obj["size"] for obj in all_objects)
        pbar = tqdm(total=total_bytes, unit="B", unit_scale=True, desc="Downloading")
        semaphore = asyncio.Semaphore(32)

        async def _download_one(obj: dict) -> None:
            path = obj["path"]
            local_path = os.path.join(local_dir, path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            async with semaphore:
                result = await obs.get_async(store, path)
                data = bytes(await result.bytes_async())

            with open(local_path, "wb") as f:
                f.write(data)

            pbar.update(obj["size"])

        await asyncio.gather(*(_download_one(obj) for obj in all_objects))
        pbar.close()

        logger.info("Download complete: %d objects", len(all_objects))
        return True
    except Exception as exc:
        logger.error("Download error: %s", exc)
        return False


def _filter_list_name(url: str) -> str:
    """Derive a short human-readable name from a filter list URL."""
    # Use the filename without extension as the name
    basename = url.rstrip("/").rsplit("/", 1)[-1]
    name = basename.rsplit(".", 1)[0] if "." in basename else basename
    return name


async def _fetch_filter_list(
    session: aiohttp.ClientSession, url: str
) -> tuple[str, str | None]:
    """Fetch a single filter list, returning (name, text) or (name, None) on error."""
    name = _filter_list_name(url)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            resp.raise_for_status()
            text = await resp.text()
            return name, text
    except Exception as exc:
        logger.error("Could not fetch %s: %s", url, exc)
        return name, None


async def build_adblocker(filter_list_urls: List[str]) -> dict[str, Any]:
    """Build one adblock Engine per filter list, keyed by list name."""
    logger.info("Fetching blocklists...")
    engines: dict[str, Any] = {}
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *(_fetch_filter_list(session, url) for url in filter_list_urls)
        )
    for name, text in results:
        if text is None:
            continue
        rules = [line.strip() for line in text.splitlines() if line.strip()]
        if rules:
            fs = adblock.FilterSet()
            fs.add_filter_list("\n".join(rules))
            engines[name] = adblock.Engine(fs)
    return engines


def process_and_enrich_data(tmp_dir: str, output_dir: str, adblocker: Any) -> None:
    logger.info(
        "Running DuckDB data enrichment (deduplicating and adding adblocker labeling)..."
    )
    con = duckdb.connect()

    enriched_dir = os.path.join(output_dir, "enriched")
    os.makedirs(enriched_dir, exist_ok=True)

    # empty view schemas to fall back to when no parquet files exist for a dtype.
    # columns must match what the downstream deduplication and join queries expect.
    _EMPTY_VIEWS = {
        "requests": """
            CREATE VIEW raw_requests AS
            SELECT
                NULL::VARCHAR AS experiment_id,
                NULL::VARCHAR AS request_id,
                NULL::VARCHAR AS worker_id,
                NULL::VARCHAR AS crawl_session_id,
                NULL::VARCHAR AS crawled_url,
                NULL::VARCHAR AS current_page_url,
                NULL::VARCHAR AS url,
                NULL::VARCHAR AS frame_url,
                NULL::VARCHAR AS resource_type,
                NULL::VARCHAR AS method,
                NULL::VARCHAR AS headers,
                NULL::VARCHAR AS timestamp,
                NULL::INTEGER AS crawl_depth
            WHERE 1=0
        """,
        "responses": """
            CREATE VIEW raw_responses AS
            SELECT
                NULL::VARCHAR AS experiment_id,
                NULL::VARCHAR AS request_id,
                NULL::VARCHAR AS crawl_session_id,
                NULL::VARCHAR AS crawled_url,
                NULL::VARCHAR AS url,
                NULL::INTEGER AS status,
                NULL::VARCHAR AS headers,
                NULL::VARCHAR AS content_type,
                NULL::VARCHAR AS timestamp,
                NULL::INTEGER AS crawl_depth
            WHERE 1=0
        """,
        "cookie_warning_consents": """
            CREATE VIEW raw_cookie_warning_consents AS
            SELECT
                NULL::VARCHAR AS experiment_id,
                NULL::VARCHAR AS crawl_session_id,
                NULL::VARCHAR AS crawled_url,
                NULL::VARCHAR AS url,
                NULL::VARCHAR AS timestamp,
                NULL::INTEGER AS crawl_depth
            WHERE 1=0
        """,
        "worker_metadata": """
            CREATE VIEW raw_worker_metadata AS
            SELECT
                NULL::VARCHAR AS worker_id,
                NULL::VARCHAR AS country_code,
                NULL::BOOLEAN AS is_residential,
                NULL::VARCHAR AS timestamp
            WHERE 1=0
        """,
        "site_metadata": """
            CREATE VIEW raw_site_metadata AS
            SELECT
                NULL::VARCHAR AS crawl_session_id,
                NULL::VARCHAR AS description,
                NULL::VARCHAR AS timestamp
            WHERE 1=0
        """,
    }

    # load raw parquet files into views from tmp
    for dtype in [
        "requests",
        "responses",
        "cookie_warning_consents",
        "worker_metadata",
        "site_metadata",
    ]:
        glob_path = os.path.join(tmp_dir, f"{dtype}_*.parquet")
        escaped_path = glob_path.replace("'", "''")
        try:
            con.execute(
                f"CREATE VIEW raw_{dtype} AS SELECT * FROM read_parquet('{escaped_path}');"
            )
        except Exception:
            con.execute(_EMPTY_VIEWS[dtype])

    # count requests per session
    con.execute("""
        CREATE VIEW session_counts AS
        SELECT crawled_url, crawl_session_id, COUNT(*) as req_count
        FROM raw_requests
        GROUP BY crawled_url, crawl_session_id
    """)

    # keep only the session with the most requests
    con.execute("""
        CREATE VIEW deduped_sessions AS
        WITH ranked_sessions AS (
            SELECT
                crawled_url,
                crawl_session_id,
                ROW_NUMBER() OVER (PARTITION BY crawled_url ORDER BY req_count DESC, crawl_session_id ASC) as rank
            FROM session_counts
        )
        SELECT crawled_url, crawl_session_id as keep_session
        FROM ranked_sessions
        WHERE rank = 1
    """)

    # deduplicate requests
    con.execute("""
        CREATE VIEW requests AS
        SELECT DISTINCT ON (r.crawled_url, r.url) r.* FROM raw_requests r
        JOIN deduped_sessions ds ON r.crawled_url = ds.crawled_url AND r.crawl_session_id = ds.keep_session
    """)

    # same for responses
    con.execute("""
        CREATE VIEW responses AS
        SELECT DISTINCT ON (r.crawled_url, r.url) r.* FROM raw_responses r
        JOIN deduped_sessions ds ON r.crawled_url = ds.crawled_url AND r.crawl_session_id = ds.keep_session
    """)

    # and cookie consents
    con.execute("""
        CREATE VIEW cookie_warning_consents AS
        SELECT DISTINCT ON (c.crawled_url) c.* FROM raw_cookie_warning_consents c
        JOIN deduped_sessions ds ON c.crawled_url = ds.crawled_url AND c.crawl_session_id = ds.keep_session
    """)

    # and site metadata
    con.execute("""
        CREATE VIEW site_metadata AS
        SELECT DISTINCT ON (sm.crawl_session_id) sm.* FROM raw_site_metadata sm
        JOIN deduped_sessions ds ON sm.crawl_session_id = ds.keep_session
    """)

    # deduplicate worker metadata (just in case)
    con.execute("""
        CREATE VIEW worker_metadata AS
        SELECT DISTINCT ON (worker_id) * FROM raw_worker_metadata
    """)

    def check_adblock(url: str, source_url: str, resource_type: str) -> str:
        if not url:
            return "[]"
        matched_lists: list[str] = []
        for name, engine in adblocker.items():
            result = engine.check_network_urls(
                url=url,
                source_url=source_url or url,
                request_type=resource_type or "other",
            )
            if result.matched:
                matched_lists.append(name)
        return json.dumps(matched_lists)

    con.create_function(
        "blocked_by",
        check_adblock,
        ["VARCHAR", "VARCHAR", "VARCHAR"],
        "VARCHAR",
    )

    # apply adblocker labeling
    con.execute("""
        CREATE TABLE labeled_requests AS
        SELECT *,
               blocked_by(url, frame_url, resource_type) AS blocked_by
        FROM requests
    """)

    logger.info(
        "Exporting enriched parquet files (default compression) to %s...",
        enriched_dir,
    )

    # export to parquet using default compression settings
    con.execute(
        f"COPY labeled_requests TO '{os.path.join(enriched_dir, 'labeled_requests.parquet')}' (FORMAT PARQUET);"
    )
    con.execute(
        f"COPY responses TO '{os.path.join(enriched_dir, 'responses.parquet')}' (FORMAT PARQUET);"
    )
    con.execute(
        f"COPY cookie_warning_consents TO '{os.path.join(enriched_dir, 'cookie_warning_consents.parquet')}' (FORMAT PARQUET);"
    )
    con.execute(
        f"COPY site_metadata TO '{os.path.join(enriched_dir, 'site_metadata.parquet')}' (FORMAT PARQUET);"
    )
    con.execute(
        f"COPY worker_metadata TO '{os.path.join(enriched_dir, 'worker_metadata.parquet')}' (FORMAT PARQUET);"
    )

    con.close()


async def async_download(
    *,
    filter_lists: Optional[str] = None,
) -> None:
    """
    Download and preprocess all crawl experiments from S3.

    :param filter_lists: comma-separated urls of filter lists to use.
    """
    output_dir = "results"
    os.makedirs(output_dir, exist_ok=True)

    config = ScriptSettings()

    filter_list_urls = (
        [url.strip() for url in filter_lists.split(",") if url.strip()]
        if filter_lists
        else DEFAULT_FILTER_LISTS
    )

    # Fetch filter lists and download from S3 concurrently
    adblocker_task = asyncio.create_task(build_adblocker(filter_list_urls))

    with tempfile.TemporaryDirectory() as tmp_dir:
        s3_task = asyncio.create_task(sync_results_from_s3(tmp_dir, config))
        adblocker, s3_ok = await asyncio.gather(adblocker_task, s3_task)

        if not s3_ok:
            logger.error("Failed to download data from object store")
            return

        # Pre-collect all worker_metadata parquet files
        system_path = os.path.join(tmp_dir, "experiment=system")
        worker_metadata_files = []
        if os.path.exists(system_path):
            for root, _, files in os.walk(system_path):
                for f in files:
                    if f.endswith(".parquet"):
                        worker_metadata_files.append(os.path.join(root, f))

        # Find all experiment directories and process each
        for entry in os.listdir(tmp_dir):
            exp_path = os.path.join(tmp_dir, entry)
            if not os.path.isdir(exp_path) or entry == "experiment=system":
                continue

            # Check if this is an experiment directory with metadata
            metadata = download_metadata(exp_path)
            if metadata is None:
                logger.debug("Skipping %s: no metadata found", entry)
                continue

            exp_id = metadata.id
            exp_output_dir = os.path.join(output_dir, exp_id)
            os.makedirs(exp_output_dir, exist_ok=True)

            # Copy metadata to output
            shutil.copy(
                os.path.join(exp_path, "metadata.json"),
                os.path.join(exp_output_dir, "metadata.json"),
            )

            # Find parquet files for this experiment
            parquet_files = []
            for root, _, files in os.walk(exp_path):
                for f in files:
                    if f.endswith(".parquet"):
                        parquet_files.append(os.path.join(root, f))

            if not parquet_files:
                logger.warning("No parquet files found for experiment %s", exp_id)
                continue

            # Copy parquet files to tmp for processing
            parquet_tmp_dir = os.path.join(tmp_dir, f"parquet_{exp_id}")
            os.makedirs(parquet_tmp_dir, exist_ok=True)

            # include worker metadata
            for pf in worker_metadata_files:
                target_name = f"worker_metadata_{os.path.basename(pf)}"
                shutil.copy(pf, os.path.join(parquet_tmp_dir, target_name))

            for pf in parquet_files:
                # Identify data type from path (e.g., data_type=requests)
                dtype = "unknown"
                for part in pf.split(os.sep):
                    if part.startswith("data_type="):
                        dtype = part.split("=")[1]
                        break

                # Prefix filename with dtype so process_and_enrich_data can find it
                target_name = f"{dtype}_{os.path.basename(pf)}"
                shutil.copy(pf, os.path.join(parquet_tmp_dir, target_name))

            process_and_enrich_data(parquet_tmp_dir, exp_output_dir, adblocker)

            logger.info("Processing complete for experiment: %s", exp_id)

    logger.info(
        "\nAll experiments processed. Results saved to: %s/",
        output_dir,
    )


def download(
    *,
    filter_lists: Optional[str] = None,
) -> None:
    """
    Download and preprocess all crawl experiments from S3.

    :param filter_lists: comma-separated urls of filter lists to use.
    """
    asyncio.run(async_download(filter_lists=filter_lists))


def main() -> None:
    defopt.run(download)


if __name__ == "__main__":
    main()
