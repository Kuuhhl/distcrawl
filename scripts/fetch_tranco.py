"""fetch tranco list."""

import logging
import sys
from pathlib import Path
from typing import Optional


import defopt
from config import ScriptSettings
from tranco import Tranco

settings = ScriptSettings()
logging.basicConfig(
    level=settings.logging_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main(
    output_path: Optional[str] = None,
) -> None:
    """
    :param output_path: path to download tranco list to.
    """
    # cache tranco
    if not output_path:
        output_path = Path("data") / "tranco_cache"
        logger.info("No output path provided. Using default path: %s", output_path)
    try:
        output_path.mkdir(parents=True, exist_ok=False)
        logger.info("Fetching full tranco list...")
        list_res = Tranco(cache_dir=str(output_path)).list()
        logger.info(
            "Done Fetching. List Date: %s, List ID: %s", list_res.date, list_res.list_id
        )

    except FileExistsError:
        logger.error(
            "There is already an instance of the tranco list cached at %s. Skipping.",
            output_path,
        )


if __name__ == "__main__":
    defopt.run(main)
