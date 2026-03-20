import asyncio
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.config.settings import get_logger
from app.crawler.browser import init_browser_context
from app.crawler.crawler import crawl_data_review
from app.parser.review_parser import extract_review
from app.parser.nlp_utils import extract_json_from_html
from app.database.crud import upsert_to_supabase
from playwright.async_api import async_playwright
from app.database.crud import get_product_links_from_supabase

logger = get_logger("ReviewPipeline")

async def process_review(browser, context_args, href):
    logger.info(f"Processing product details for: {href}")
    raw_data = await crawl_data_review(browser, context_args, href)
    
    for idx, item in enumerate(raw_data):
        if item.get("type") == "html":
            try:
                chunks = await asyncio.to_thread(extract_json_from_html, item.get("data", ""), item.get("url", ""))
                raw_data.extend(chunks)
            except Exception as e:
                logger.error(f"HTML parse error: {e}")

    structured_data = await extract_review(raw_data)
        
    await upsert_to_supabase(structured_data)


async def run_pipeline():
    async with async_playwright() as p:
        browser, context, context_args = await init_browser_context(p)
        logger.info("Pipeline started")

        #Gọi supabase và lấy link các sản phẩm
        product_links = await get_product_links_from_supabase()
        for product_link in product_links:
            await process_review(browser, context_args, product_link)
        
        await context.close()
        await browser.close()
        logger.info("Pipeline finished successfully")

if __name__ == "__main__":
    asyncio.run(run_pipeline())