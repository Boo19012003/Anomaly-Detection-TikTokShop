import asyncio
from typing import Dict, List
from app.config.settings import get_logger, UPSERT_BATCH_SIZE, UPSERT_FLUSH_INTERVAL
from app.database.crud import upsert_to_supabase

logger = get_logger("UpsertQueue")

PRIMARY_KEYS = {
    "shops": "shop_id",
    "products": "product_url",
    "reviews": "review_id",
    "products_metrics_history": None 
}

class UpsertQueue:
    def __init__(
        self,
        batch_size: int = UPSERT_BATCH_SIZE,
        flush_interval: int = UPSERT_FLUSH_INTERVAL,
        max_retries: int = 3,
        on_flush=None
    ):
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._max_retries = max_retries
        self.on_flush = on_flush

        self._buffer: Dict[str, Dict[str, dict]] = {}
        self._list_buffer: Dict[str, List[dict]] = {}
        self._url_buffer: set = set()
        
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None

        self.stats = {"upserted": 0, "failed": 0, "retries": 0}

    async def __aenter__(self):
        self._flush_task = asyncio.create_task(self._auto_flush_loop())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._flush_task:
            self._flush_task.cancel()
        await self.flush_all()
        return False

    async def add(self, structured_data: dict, url: str = None):
        """Thêm dữ liệu vào bộ đệm và lọc trùng lặp ngay từ lúc thêm (DE logic)"""
        if not isinstance(structured_data, dict):
            return

        async with self._lock:
            if url:
                self._url_buffer.add(url)
            for table_name, items in structured_data.items():
                if not items:
                    continue
                    
                if not isinstance(items, list):
                    items = [items]

                pk_field = PRIMARY_KEYS.get(table_name)

                if pk_field:
                    if table_name not in self._buffer:
                        self._buffer[table_name] = {}
                    for item in items:
                        if pk_field in item:
                            self._buffer[table_name][item[pk_field]] = item 
                else:
                    if table_name not in self._list_buffer:
                        self._list_buffer[table_name] = []
                    self._list_buffer[table_name].extend(items)

        await self._check_and_flush()

    async def _check_and_flush(self):
        """Chỉ flush những bảng đã đủ số lượng, không flush toàn bộ"""
        tables_to_flush = {}
        urls_to_flush = []
        
        async with self._lock:
            for table, records in list(self._buffer.items()):
                if len(records) >= self._batch_size:
                    tables_to_flush[table] = list(records.values())
                    self._buffer[table].clear()

            for table, records in list(self._list_buffer.items()):
                if len(records) >= self._batch_size:
                    tables_to_flush[table] = list(records)
                    self._list_buffer[table].clear()

            if tables_to_flush:
                urls_to_flush = list(self._url_buffer)
                self._url_buffer.clear()

        if tables_to_flush:
            await self._execute_upsert_with_retry(tables_to_flush, urls_to_flush)

    async def flush_all(self):
        """Ép buộc xả toàn bộ dữ liệu đang có (Dùng khi shutdown pipeline)"""
        tables_to_flush = {}
        urls_to_flush = []
        async with self._lock:
            for table, records in list(self._buffer.items()):
                if records:
                    tables_to_flush[table] = list(records.values())
                    self._buffer[table].clear()
            for table, records in list(self._list_buffer.items()):
                if records:
                    tables_to_flush[table] = list(records)
                    self._list_buffer[table].clear()
            if tables_to_flush:
                urls_to_flush = list(self._url_buffer)
                self._url_buffer.clear()

        if tables_to_flush:
            await self._execute_upsert_with_retry(tables_to_flush, urls_to_flush)

    async def _execute_upsert_with_retry(self, batch: dict, urls: list = None):
        """Hành động quan trọng của DE: Đẩy data lên DB và có cơ chế Retry/Rollback"""
        total_items = sum(len(v) for v in batch.values())
        logger.info(f"Uploading batch of {total_items} records...")

        for attempt in range(1, self._max_retries + 1):
            success = await upsert_to_supabase(batch)
            if success:
                self.stats["upserted"] += total_items
                if self.on_flush and urls:
                    try:
                        if asyncio.iscoroutinefunction(self.on_flush):
                            await self.on_flush(urls)
                        else:
                            self.on_flush(urls)
                    except Exception as e:
                        logger.error(f"Error in on_flush callback: {e}")
                return True
            
            self.stats["retries"] += 1
            logger.warning(f"Upsert failed! Retrying {attempt}/{self._max_retries} in 2 seconds...")
            await asyncio.sleep(2)

        self.stats["failed"] += total_items
        logger.error("Max retries reached. Saving failed batch to Dead Letter Queue (DLQ.json)...")
        self._save_to_dlq(batch)
        return False

    def _save_to_dlq(self, batch: dict):
        import json, time
        filename = f"dlq_batch_{int(time.time())}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(batch, f, ensure_ascii=False)

    async def _auto_flush_loop(self):
        try:
            while True:
                await asyncio.sleep(self._flush_interval)
                await self.flush_all()
        except asyncio.CancelledError:
            pass