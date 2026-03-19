import asyncio
import json
from playwright.async_api import async_playwright

from app.config.settings import get_logger
from app.crawler.browser import check_captcha_visible, solve_captcha_async, init_browser_context
from app.crawler.consumer import crarwl_data_product
from app.parser.review_parser import extract_tiktok_data, extract_json_from_html_sync
from app.database.crud import upsert_to_supabase

logger = get_logger("PipelineOrchestrator")

async def process_product_url(browser, context_args, href):
    logger.info(f"Processing product details for: {href}")
    raw_data = await crarwl_data_product(browser, context_args, href)
    
    for idx, item in enumerate(raw_data):
        if item.get("type") == "html":
            try:
                chunks = await asyncio.to_thread(extract_json_from_html_sync, item.get("data", ""), item.get("url", ""))
                raw_data.extend(chunks)
            except Exception as e:
                logger.error(f"HTML parse error: {e}")

    structured_data = await extract_tiktok_data(raw_data)
    await upsert_to_supabase(structured_data)

async def collect_url_product(browser, context_args, name, url, semaphore):
    """
    Crawls a category url sequentially taking products and immediately processes them.
    """
    async with semaphore:
        context = await browser.new_context(**context_args)
        page = await context.new_page()
        cat_links_list = []
        try:
            await page.goto(url, timeout=10000)
            while True:
                if await check_captcha_visible(page):
                    await solve_captcha_async(page)
                else:
                    if await page.locator('div.flex.justify-center.mt-16:has-text("No more products")').is_visible():
                        break
                    else:
                        view_more = page.locator('div.flex.justify-center.mt-16:has-text("View more")')
                        if await view_more.is_visible():
                            await view_more.click(timeout=2000)
                            await page.wait_for_timeout(3000)
                        else:
                            break

            product_cards = await page.query_selector_all("div[class*='rounded']:has(a[href*='/pdp/'])")
            for card in product_cards:
                link_el = await card.query_selector("a[href*='shop/vn/pdp/']")
                if link_el:
                    href = await link_el.get_attribute("href")
                    if href and "http" not in href:
                        href = "https://www.tiktok.com" + href
                    cat_links_list.append(href)
                    
                    # Processing directly
                    await process_product_url(browser, context_args, href)

            logger.info(f"✅ {name} collected & sequentially processed {len(cat_links_list)} links")
            return cat_links_list

        except Exception as e:
            logger.error(f"{name} error: {e}")
        finally:
            await page.close()
            await context.close()

async def run_pipeline():
    async with async_playwright() as p:
        browser, context, context_args = await init_browser_context(p)
        logger.info("Pipeline started... Testing with 1 URL mode")
        
        test_url = "https://www.tiktok.com/shop/vn/pdp/dau-goi-thao-duoc-namnung-chiet-xuat-thien-nhien-185ml/1730089185693370633"
        await process_product_url(browser, context_args, test_url)
        
        await context.close()
        await browser.close()
