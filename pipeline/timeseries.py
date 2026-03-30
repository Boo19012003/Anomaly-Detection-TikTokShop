import asyncio
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.config.settings import get_logger
from app.crawler.browser import init_browser_context, intercept_route
from app.crawler.crawler import crawl_data_product
from app.parser.timeseries_parser import extract_timeseries
from app.parser.nlp_utils import extract_json_from_html
from app.database.crud import upsert_to_supabase
from playwright.async_api import async_playwright
from app.database.crud import get_product_links_from_supabase

logger = get_logger("TimeseriesPipeline")

async def process_timeseries(browser, url, semaphore):
    task_logger = logger.bind(target_url=url)
    task_logger.info("Start processing timeseries")
    try:
        raw_data = await crawl_data_product(browser, url, semaphore)
        
        for idx, item in enumerate(raw_data):
            if item.get("type") == "html":
                try:
                    chunks = await asyncio.to_thread(extract_json_from_html, item.get("data", ""), item.get("url", ""))
                    raw_data.extend(chunks)
                except Exception as e:
                    task_logger.exception("HTML parse error")

        structured_data = await extract_timeseries(raw_data)
            
        await upsert_to_supabase(structured_data)
        task_logger.info("Successfully processed timeseries")
    except Exception as e:
        task_logger.exception("Failed to process timeseries")


async def run_pipeline():
    async with async_playwright() as p:
        browser, context, _ = await init_browser_context(p)
        await context.route("**/*", intercept_route)
        logger.info("Pipeline started")

        product_links = await get_product_links_from_supabase()
        logger.info(f"Found {len(product_links)} product links")
        MAX_PRODUCT_TABS = 3
        product_semaphore = asyncio.Semaphore(MAX_PRODUCT_TABS)
        tasks = [
            asyncio.create_task(process_timeseries(context, product_link, product_semaphore)) 
            for product_link in product_links
        ]
        await asyncio.gather(*tasks)
        
        await context.close()
        await browser.close()
        logger.info("Pipeline finished successfully")

if __name__ == "__main__":
    asyncio.run(run_pipeline())