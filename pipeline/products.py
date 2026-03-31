import asyncio
import sys
import os
import re
import json
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.config.settings import get_logger, MAX_CONCURRENT_PAGES
from app.crawler.browser import handle_captcha, init_browser_context, intercept_route
from app.crawler.crawler import crawl_data_product
from app.parser.product_parser import extract_product
from app.parser.nlp_utils import extract_json_from_html
from app.database.upsert_queue import UpsertQueue

logger = get_logger("ProductPipeline")

async def process_product_url(context, href, semaphore, queue):
    task_logger = logger.bind(target_url=href)
    try:
        raw_data = await crawl_data_product(context, href, semaphore)
        
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

        structured_data = await extract_product(raw_data)
        raw_data.clear()
        del raw_data

        await queue.add(structured_data)
        del structured_data
        task_logger.debug("Successfully queued product data")
        return True

    except Exception as e:
        task_logger.exception("Failed to process product URL")
        return False

async def collect_url_product(context, name, url, semaphore):
    async with semaphore:
        page = await context.new_page()
        product_links = set()

        try:
            await page.goto(url, timeout=30000)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=5000)
            except PlaywrightTimeoutError:
                pass

            await handle_captcha(page, wait_ms=500)

            
            view_more_btn = page.locator('div.flex.justify-center.mt-16:has-text("View more")')
            no_more_products = page.locator('div.flex.justify-center.mt-16:has-text("No more products")')

            product_locator = page.locator('a[href*="/pdp/"]')

            while True:
                await handle_captcha(page, wait_ms=500)
                
                current_links = await product_locator.evaluate_all("elements => elements.map(el => el.href)")
                product_links.update(current_links)

                current_product_count = len(current_links)

                if await no_more_products.is_visible():
                    logger.debug(f"[{name}] No more products")
                    break

                if await view_more_btn.is_visible():
                    try:
                        await view_more_btn.click(timeout=3000)
                        
                        try:
                            await page.wait_for_function(
                                f"document.querySelectorAll('a[href*=\"/pdp/\"]').length > {current_product_count}",
                                timeout=5000
                            )
                        except PlaywrightTimeoutError:
                            logger.debug(f"[{name}] Unable to load new products, checking captcha.")
                            if not await handle_captcha(page, wait_ms=500):
                                pass
                    
                    except (PlaywrightTimeoutError, PlaywrightError) as e:
                        logger.debug(f"[{name}] Error click view more: {e}")
                        if not await handle_captcha(page, wait_ms=500):

                            await page.wait_for_timeout(1000)

                else:
                    await page.wait_for_timeout(1000)
                    if not await handle_captcha(page, wait_ms=500):
                         if not await view_more_btn.is_visible() and not await no_more_products.is_visible():
                             logger.warning(f"[{name}] No next state found. Exit loop.")
                             break
                
            logger.info(f"Found {len(product_links)} product links in category {name}")
            return list(product_links)

        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout analyzing {name}: {e}")
        except PlaywrightError as e:
            logger.error(f"Playwright error analyzing {name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in {name}: {e}")
        finally:
            await page.close()

async def collect_category_links(context_args, browser):
    context = await browser.new_context(**context_args)
    await context.route("**/*", intercept_route)
    home_page = await context.new_page()
    await home_page.goto("https://www.tiktok.com/shop/vn", timeout=30000)
    
    try:
        await home_page.wait_for_load_state("networkidle", timeout=5000)
    except PlaywrightTimeoutError:
        pass

    await handle_captcha(home_page)

    try:
        await home_page.wait_for_selector('a[href*="/c/"]', timeout=10000)
    except PlaywrightTimeoutError:
        logger.warning("Timeout waiting to find category links")
    except Exception as e:
        logger.warning(f"Unexpected error finding category links: {e}")

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
        
        browser, _, context_args = await init_browser_context(p)

        try:
            # --- Phase 1: Collect category links ---
            cat_links = await collect_category_links(context_args, browser)

            if not cat_links:
                logger.error("No category links found.")
                return

            # --- Phase 2: Collect product URLs from categories ---
            context_2 = await browser.new_context(**context_args)
            await context_2.route("**/*", intercept_route)
            
            cat_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES) 
            
            tasks_2 = [
                asyncio.create_task(collect_url_product(context_2, cat["name"], cat["url"], cat_semaphore)) 
                for cat in cat_links
            ]
            
            nested_product_links = await asyncio.gather(*tasks_2)
            del tasks_2, cat_links

            product_links = [url for sublist in nested_product_links if sublist for url in sublist]
            del nested_product_links
            
            await context_2.close()
            logger.info(f"{len(product_links)} product links collected.")

            if not product_links:
                logger.error("No product links found.")
                return

            # --- Phase 3: Process each product URL ---
            context_3 = await browser.new_context(**context_args)
            await context_3.route("**/*", intercept_route)

            product_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
            
            async with UpsertQueue() as queue:
                tasks_3 = [
                    asyncio.create_task(process_product_url(context_3, href, product_semaphore, queue)) 
                    for href in product_links
                ]
                del product_links
                results = await asyncio.gather(*tasks_3)
                del tasks_3
            
            success_count = sum(1 for r in results if not isinstance(r, Exception))
            fail_count = len(results) - success_count

            await context_3.close()
            
            logger.info(
                f"Pipeline finished. Processed: {len(results)} products | "
                f"Success: {success_count} | Failed: {fail_count}"
            )
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(run_pipeline())