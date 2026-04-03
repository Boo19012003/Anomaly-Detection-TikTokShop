import asyncio
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.config.settings import get_logger, MAX_CONCURRENT_PAGES
from app.crawler.browser import init_browser_context, intercept_route
from app.crawler.crawler import crawl_data_product
from app.parser.timeseries_parser import extract_timeseries
from app.parser.nlp_utils import extract_json_from_html
from app.database.upsert_queue import UpsertQueue
from app.database.crud import get_uncrawled_product_links_from_supabase
from playwright.async_api import async_playwright

logger = get_logger("TimeseriesPipeline")

async def process_timeseries(browser, url, semaphore, queue):
    task_logger = logger.bind(target_url=url)
    try:
        raw_data = await crawl_data_product(browser, url, semaphore)
        
        for idx, item in enumerate(raw_data):
            if item.get("type") == "html":
                try:
                    chunks = await asyncio.to_thread(extract_json_from_html, item.get("data", ""), item.get("url", ""))
                    raw_data.extend(chunks)
                    del chunks
                except (json.JSONDecodeError, ValueError, TypeError) as e:
                    task_logger.error(f"HTML parse data formatting error: {e}")
                except Exception as e:
                    task_logger.exception(f"Unexpected HTML parse error: {e}")
                    
        structured_data = await extract_timeseries(raw_data)
        raw_data.clear()
        del raw_data

        await queue.add(structured_data)
        del structured_data
        task_logger.debug("Successfully queued timeseries data")
        return True

    except Exception as e:
        task_logger.exception("Failed to process timeseries")
        return False

async def run_pipeline():
    async with async_playwright() as p:
        browser, context, _ = await init_browser_context(p)
        await context.route("**/*", intercept_route)
        logger.info("Pipeline started")

        total_processed = 0
        total_success = 0
        total_fail = 0

        async with UpsertQueue() as queue:
            while True:
                product_links = await get_uncrawled_product_links_from_supabase(50) 

                if not product_links:
                    logger.info("No product links found.")
                    break

                logger.info(f"Found {len(product_links)} product links")

                product_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
                tasks = [
                    asyncio.create_task(process_timeseries(context, product_link, product_semaphore, queue)) 
                    for product_link in product_links
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                batch_success = sum(1 for r in results if not isinstance(r, Exception))
                batch_fail = len(results) - batch_success
                
                total_processed += len(results)
                total_success += batch_success
                total_fail += batch_fail

        await context.close()
        await browser.close()
        
        logger.info(
            f"Pipeline finished. Processed: {total_processed} products | "
            f"Success: {total_success} | Failed: {total_fail}"
        )

if __name__ == "__main__":
    asyncio.run(run_pipeline())