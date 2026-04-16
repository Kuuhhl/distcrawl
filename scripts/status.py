"""live experiment status dashboard using S3 object store and nats."""

import asyncio
import re
import json
import logging
import sys
from typing import Any, Dict, List, Optional

import defopt
import obstore as obs
from dist_common import ExperimentMetadata
from dist_common.config import ObjectStore
from rich.console import Console
from rich.live import Live
from rich.table import Table
from config import ScriptSettings

settings = ScriptSettings()
logging.basicConfig(
    level=settings.logging_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def _list_experiment_ids(storage: ObjectStore) -> List[str]:
    try:
        objs = []
        stream = obs.list(storage)
        async for chunk in stream:
            objs.extend(chunk)

        pattern = re.compile(r"experiment=(.*?)/metadata\.json")

        matches = {
            pattern.search(obj["path"]).group(1)
            for obj in objs
            if pattern.search(obj["path"])
        }

        return sorted(list(matches), reverse=True)
    except Exception:
        return []


async def _load_metadata(
    storage: ObjectStore, exp_id: str
) -> Optional[ExperimentMetadata]:
    try:
        key = f"experiment={exp_id}/metadata.json"
        result = await obs.get_async(storage, key)
        data = await result.bytes_async()
        raw = json.loads(bytes(data).decode("utf-8"))
        return ExperimentMetadata(**raw)
    except Exception as exc:
        logger.debug("Could not load metadata for %s: %s", exp_id, exc)
        return None


async def _get_nats_remaining(
    nc: Any,
    config: ScriptSettings,
    exp_ids: List[str],
) -> Dict[str, int]:
    counts: Dict[str, int] = {eid: -1 for eid in exp_ids}
    try:
        js = nc.jetstream()

        for exp_id in exp_ids:
            subject = f"{config.subject_prefix}.*.*.{exp_id}"
            try:
                info = await js.stream_info(config.stream_name, subjects_filter=subject)
                subjects = info.state.subjects or {}
                total_remaining = sum(count for subj, count in subjects.items())
                counts[exp_id] = total_remaining
            except Exception:
                counts[exp_id] = 0

    except Exception:
        pass

    return counts


async def get_experiment_status(
    storage: ObjectStore, nc: Any, config: ScriptSettings
) -> Dict[str, dict]:
    exp_ids = await _list_experiment_ids(storage)

    experiments: Dict[str, dict] = {}
    for exp_id in exp_ids:
        meta = await _load_metadata(storage, exp_id)
        experiments[exp_id] = {
            "id": exp_id,
            "metadata": meta,
            "total": meta.total_urls if meta else 0,
            "remaining": -1,
            "completed": 0,
        }

    nats_counts = await _get_nats_remaining(nc, config, exp_ids)

    for exp_id, data in experiments.items():
        remaining = nats_counts.get(exp_id, -1)
        data["remaining"] = remaining
        if remaining >= 0 and data["total"] > 0:
            data["completed"] = data["total"] - remaining
        else:
            data["completed"] = 0

    return experiments


def generate_table(experiments: Dict[str, dict]) -> Table:
    table = Table(title="Crawl Experiments Status")
    table.add_column("Exp ID", style="cyan", no_wrap=True)
    table.add_column("Status", style="green")
    table.add_column("Progress", style="magenta", justify="right")
    table.add_column("Done / Total", justify="right")
    table.add_column("Created", style="dim")

    for exp_id, data in sorted(experiments.items(), reverse=True):
        meta: Optional[ExperimentMetadata] = data["metadata"]
        total: int = data["total"]
        completed: int = data["completed"]
        remaining: int = data["remaining"]

        if total == 0:
            status = "[red]Error/Empty"
            progress_str = "N/A"
        elif remaining == -1:
            status = "[red]NATS N/A"
            progress_str = "N/A"
        elif remaining == 0:
            status = "[green]Done"
            progress_str = "100.0%"
        else:
            status = "[yellow]Running"
            pct = completed / total * 100
            progress_str = f"{pct:.1f}%"

        done_total = f"{completed}/{total}" if remaining != -1 else f"?/{total}"
        created_str = meta.timestamp.strftime("%Y-%m-%d %H:%M") if meta else "Unknown"

        table.add_row(
            exp_id,
            status,
            progress_str,
            done_total,
            created_str,
        )

    return table


async def async_main(config: ScriptSettings) -> None:
    import nats

    console = Console()
    nc = await nats.connect(config.nats_url, token=config.nats_token)

    try:
        storage = config.get_storage()

        with Live(generate_table({}), refresh_per_second=1, console=console) as live:
            while True:
                experiments = await get_experiment_status(storage, nc, config)
                live.update(generate_table(experiments))
                await asyncio.sleep(10)
    finally:
        await nc.close()


def status(
    *,
    nats_url: Optional[str] = None,
    results_bucket: Optional[str] = None,
) -> None:
    overrides = {
        k: v
        for k, v in {
            "nats_url": nats_url,
            "results_bucket": results_bucket,
        }.items()
        if v is not None
    }
    config = ScriptSettings(**overrides)

    try:
        asyncio.run(async_main(config))
    except KeyboardInterrupt:
        pass


def main() -> None:
    defopt.run(status)


if __name__ == "__main__":
    main()
