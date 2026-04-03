import asyncio
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.config.settings import get_logger, MAX_CONCURRENT_PAGES
from app.crawler.browser import init_browser_context, intercept_route
from app.crawler.crawler import crawl_data_review
from app.parser.review_parser import extract_review
from app.parser.nlp_utils import extract_json_from_html
from app.database.upsert_queue import UpsertQueue
from app.database.crud import get_uncrawled_product_links_from_supabase, mark_product_as_crawled
from playwright.async_api import async_playwright

logger = get_logger("ReviewPipeline")

async def process_review(context, url, semaphore, queue):
    task_logger = logger.bind(target_url=url)
    try:
        raw_data = await crawl_data_review(context, url, semaphore)
        
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

        structured_data = await extract_review(raw_data)
        raw_data.clear()
        del raw_data
                
        await queue.add(structured_data, url=url)
        del structured_data
        task_logger.info("Successfully queued review data")
        return True
    except Exception as e:
        task_logger.exception("Failed to process reviews")
        return False

async def _mark_crawled_callback(urls: list[str]):
    for url in urls:
        await mark_product_as_crawled(url)

async def run_pipeline():
    async with async_playwright() as p:
        browser, context, _ = await init_browser_context(p)
        await context.route("**/*", intercept_route)
        logger.info("Pipeline started")

        total_processed = 0
        total_success = 0
        total_fail = 0

        async with UpsertQueue(on_flush=_mark_crawled_callback) as queue:
            while True:
                product_links = await get_uncrawled_product_links_from_supabase(limit=50)

                if not product_links:
                    logger.info("No product links found. Retrying in 10 minutes...")
                    await asyncio.sleep(600)
                    continue

                logger.info(f"Processing batch of {len(product_links)} products")
                review_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
                tasks = [
                    asyncio.create_task(process_review(context, product_link, review_semaphore, queue)) 
                    for product_link in product_links
                ]
                del product_links
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                del tasks
                batch_success = sum(1 for r in results if not isinstance(r, Exception))
                batch_fail = len(results) - batch_success
                
                total_processed += len(results)
                total_success += batch_success
                total_fail += batch_fail
                del results

        await context.close()
        await browser.close()

        logger.info(
            f"Pipeline finished. Processed: {total_processed} products | "
            f"Success: {total_success} | Failed: {total_fail}"
        )

if __name__ == "__main__":
    asyncio.run(run_pipeline())