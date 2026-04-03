import asyncio
from app.config.settings import get_logger, UPSERT_BATCH_SIZE, UPSERT_FLUSH_INTERVAL
from app.database.crud import upsert_to_supabase

logger = get_logger("UpsertQueue")

TABLE_KEYS = ("shops", "products", "products_metrics_history", "reviews")


class UpsertQueue:

    def __init__(
        self,
        batch_size: int = UPSERT_BATCH_SIZE,
        flush_interval: int = UPSERT_FLUSH_INTERVAL,
        on_flush=None,
    ):
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._on_flush = on_flush

        # Per-table accumulation buffer
        self._buffer: dict[str, list] = {key: [] for key in TABLE_KEYS}
        # URLs attached to buffered data (used by review pipeline callback)
        self._pending_urls: list[str] = []

        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._item_count = 0

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self):
        await self.stop()
        return False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        """Start the periodic auto-flush background task."""
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._auto_flush_loop())
            logger.debug(
                f"UpsertQueue started (batch_size={self._batch_size}, "
                f"flush_interval={self._flush_interval}s)"
            )

    async def stop(self):
        """Flush remaining data and cancel the background task."""
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None

        # Final flush to make sure nothing is left in the buffer
        await self.flush()
        logger.debug("UpsertQueue stopped")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def add(self, structured_data: dict, url: str | None = None):
        """Add *structured_data* to the internal buffer.

        If *url* is provided it will be collected and forwarded to the
        *on_flush* callback after the batch that contains it is persisted.
        """
        if not isinstance(structured_data, dict):
            logger.warning(f"Ignoring non-dict data: {type(structured_data)}")
            return

        async with self._lock:
            for key in TABLE_KEYS:
                items = structured_data.get(key)
                if items:
                    if isinstance(items, list):
                        self._buffer[key].extend(items)
                        self._item_count += len(items)
                    else:
                        self._buffer[key].append(items)
                        self._item_count += 1

            if url is not None:
                self._pending_urls.append(url)

        # Flush immediately if buffer exceeds batch size
        if self._item_count >= self._batch_size:
            await self.flush()

    async def flush(self):
        """Merge the buffer into a single dict and upsert to Supabase."""
        async with self._lock:
            if self._item_count == 0:
                return

            # Snapshot and reset
            batch = {key: list(self._buffer[key]) for key in TABLE_KEYS if self._buffer[key]}
            flushed_urls = list(self._pending_urls)
            flushed_count = self._item_count

            for key in TABLE_KEYS:
                self._buffer[key].clear()
            self._pending_urls.clear()
            self._item_count = 0

        # Perform the actual upsert outside the lock
        logger.info(f"Flushing batch of {flushed_count} items to Supabase")
        success = await upsert_to_supabase(batch)

        if success and self._on_flush and flushed_urls:
            try:
                await self._on_flush(flushed_urls)
            except Exception as e:
                logger.error(f"on_flush callback error: {e}")

        return success

    # ------------------------------------------------------------------
    # Background auto-flush
    # ------------------------------------------------------------------
    async def _auto_flush_loop(self):
        """Periodically flush the buffer regardless of size."""
        try:
            while True:
                await asyncio.sleep(self._flush_interval)
                if self._item_count > 0:
                    logger.debug(f"Auto-flush triggered ({self._item_count} items buffered)")
                    await self.flush()
        except asyncio.CancelledError:
            pass
