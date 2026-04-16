"""batched parquet batcher."""

import asyncio
import io
import logging
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Mapping

import obstore as obs
import pyarrow as pa
import pyarrow.parquet as pq
from dist_common.config import ObjectStore

logger = logging.getLogger(__name__)


class ParquetBatcher:
    """batches data and converts it to parquet bytes."""

    def __init__(
        self,
        storage: ObjectStore,
        batch_size: int,
    ) -> None:
        self.storage = storage
        self.batch_size = batch_size
        self.buffers: Dict[str, Dict[str, List[Mapping[str, Any]]]] = defaultdict(
            lambda: defaultdict(list)
        )

    async def append(
        self, exp_id: str, data_type: str, item: Mapping[str, Any]
    ) -> None:
        """buffer a record."""
        self.buffers[exp_id][data_type].append(item)

        if len(self.buffers[exp_id][data_type]) >= self.batch_size:
            await self.flush_buffer(exp_id, data_type)

    async def flush_buffer(self, exp_id: str, data_type: str) -> None:
        """serialize buffer to parquet and put to storage."""
        if exp_id not in self.buffers or data_type not in self.buffers[exp_id]:
            return

        data = self.buffers[exp_id][data_type]
        if not data:
            return

        # snapshot and clear
        self.buffers[exp_id][data_type] = []

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = "experiment=%s/data_type=%s/%s_%s.parquet" % (
            exp_id,
            data_type,
            timestamp,
            uuid.uuid4().hex[:8],
        )

        logger.info("Flushing %d records to %s", len(data), path)

        # conversion to parquet bytes
        try:
            table = pa.Table.from_pylist(data)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)
            parquet_data = buf.read()

            await obs.put_async(self.storage, path, parquet_data)
        except Exception as exc:
            logger.error("Failed to serialize or put parquet: %s", exc)
            raise

    async def flush_all(self) -> None:
        """flush all buffers."""
        tasks = []
        for exp_id in list(self.buffers.keys()):
            for data_type in list(self.buffers[exp_id].keys()):
                tasks.append(self.flush_buffer(exp_id, data_type))

        if tasks:
            await asyncio.gather(*tasks)
