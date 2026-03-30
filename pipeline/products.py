import asyncio
import sys
import os
import re
import json
from playwright.async_api import async_playwright

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.config.settings import get_logger
from app.crawler.browser import check_captcha_visible, solve_captcha_async, init_browser_context, intercept_route
from app.crawler.crawler import crawl_data_product
from app.parser.product_parser import extract_product
from app.parser.nlp_utils import extract_json_from_html
from app.database.crud import upsert_to_supabase

logger = get_logger("ProductPipeline")

async def process_product_url(context, href, semaphore):
    task_logger = logger.bind(target_url=href)
    try:
        raw_data = await crawl_data_product(context, href, semaphore)
        
        for idx, item in enumerate(raw_data):
            if item.get("type") == "html":
                try:
                    chunks = await asyncio.to_thread(extract_json_from_html, item.get("data", ""), item.get("url", ""))
                    raw_data.extend(chunks)
                except Exception as e:
                    task_logger.exception("HTML parse error")

        structured_data = await extract_product(raw_data)
        await upsert_to_supabase(structured_data)
        task_logger.info("Successfully processed product URL")
    except Exception as e:
        task_logger.exception("Failed to process product URL")

async def collect_url_product(context, name, url, semaphore):
    async with semaphore:
        page = await context.new_page()
        product_links = set()

        try:
            await page.goto(url, timeout=30000)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except:
                pass

            if await check_captcha_visible(page):
                await solve_captcha_async(page)
                await page.wait_for_timeout(500)
            
            view_more_btn = page.locator('div.flex.justify-center.mt-16:has-text("View more")')
            no_more_products = page.locator('div.flex.justify-center.mt-16:has-text("No more products")')
            while True:
                if await check_captcha_visible(page):
                    try:
                        await solve_captcha_async(page)
                        await page.wait_for_timeout(500)
                    except:
                        pass

                if await no_more_products.is_visible():
                    break

                if not await view_more_btn.is_visible():
                    await page.wait_for_timeout(500)
                    if await check_captcha_visible(page):
                        continue
                    elif not await view_more_btn.is_visible():
                        break

                try:
                    await view_more_btn.click(timeout=2000) 
                    await page.wait_for_timeout(500)

                except Exception:
                    if await check_captcha_visible(page):
                        continue
                    else:
                        break
            
            await page.wait_for_selector('a[href*="/pdp/"]', timeout=10000)
            
            links = await page.locator('a[href*="/pdp/"]').evaluate_all(
                "elements => elements.map(el => el.href)"
            )
            
            for link in links:
                product_links.add(link)
                
            logger.info(f"Lấy được {len(product_links)} link sản phẩm từ danh mục {name}")
            return list(product_links)

        except Exception as e:
            logger.error(f"{name} error: {e}")
        finally:
            await page.close()

async def collect_category_links(browser, context_args):
    context = await browser.new_context(**context_args)
    await context.route("**/*", intercept_route)
    home_page = await context.new_page()
    await home_page.goto("https://www.tiktok.com/shop/vn", timeout=30000)
    
    try:
        await home_page.wait_for_load_state("networkidle", timeout=5000)
    except Exception:
        pass

    if await check_captcha_visible(home_page):
        await solve_captcha_async(home_page)

    try:
        await home_page.wait_for_selector('a[href*="/c/"]', timeout=10000)
    except Exception:
        logger.warning("Not found category links")

    target_categories = [
        "Womenswear & Underwear", 
        "Menswear & Underwear", 
        "Beauty & Personal Care"
    ]
    cat_links = []
    
    cat_elements = await home_page.query_selector_all('a[href*="/c/"]')
    
    for cat in cat_elements:
        name = (await cat.inner_text()).strip()
        if name in target_categories:
            url = await cat.get_attribute('href')
            cat_links.append({"name": name, "url": url})
    
    logger.info(f"Taked {len(cat_links)} target categories.")
    for cat in cat_links:
        logger.info(f"Category: {cat['name']} - {cat['url']}")

    await home_page.close()
    await context.close()
    return cat_links

async def run_pipeline():
    async with async_playwright() as p:
        logger.info("Pipeline started")
        
        browser_1, _, context_args_1 = await init_browser_context(p)
        cat_links = await collect_category_links(browser_1, context_args_1)
        await browser_1.close()

        if not cat_links:
            logger.error("No category links found.")
            return

        browser_2, context_2, _ = await init_browser_context(p)
        await context_2.route("**/*", intercept_route)
        
        MAX_CAT_TABS = 3
        cat_semaphore = asyncio.Semaphore(MAX_CAT_TABS) 
        
        tasks_2 = [
            asyncio.create_task(collect_url_product(context_2, cat["name"], cat["url"], cat_semaphore)) 
            for cat in cat_links
        ]
        
        nested_product_links = await asyncio.gather(*tasks_2)
        product_links = [url for sublist in nested_product_links if sublist for url in sublist]
        
        await browser_2.close()
        logger.info(f"{len(product_links)} product links collected.")

        if not product_links:
            logger.error("No product links found.")
            return

        browser_3, context_3, _ = await init_browser_context(p)
        await context_3.route("**/*", intercept_route)

        MAX_PRODUCT_TABS = 3
        product_semaphore = asyncio.Semaphore(MAX_PRODUCT_TABS)
        
        tasks_3 = [
            asyncio.create_task(process_product_url(context_3, href, product_semaphore)) 
            for href in product_links
        ]
        await asyncio.gather(*tasks_3)
        
        await browser_3.close()
        
        logger.info("Pipeline finished successfully")

if __name__ == "__main__":
    asyncio.run(run_pipeline())