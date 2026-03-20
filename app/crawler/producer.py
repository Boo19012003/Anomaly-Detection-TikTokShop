import asyncio
import json
from playwright.async_api import async_playwright

from app.config.settings import get_logger
from app.crawler.browser import check_captcha_visible, solve_captcha_async, init_browser_context, intercept_route
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

    with open(f"raw_data.json", "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=4)

    with open(f"Structured_data.json", "w", encoding="utf-8") as f:
        json.dump(structured_data, f, ensure_ascii=False, indent=4)
        
    await upsert_to_supabase(structured_data)

async def collect_url_product(browser, context_args, name, url, semaphore):
    """
    Crawls a category url sequentially taking products and immediately processes them.
    """
    async with semaphore:
        context = await browser.new_context(**context_args)
        await context.route("**/*", intercept_route)
        page = await context.new_page()
        cat_links_list = []
        try:
            await page.goto(url, timeout=30000)
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

            logger.info(f"{name} collected & sequentially processed {len(cat_links_list)} links")
            return cat_links_list

        except Exception as e:
            logger.error(f"{name} error: {e}")
        finally:
            await page.close()
            await context.close()

async def collect_category_links(browser, context_args):
    """
    Đi tới trang chủ, giải captcha (nếu có) và thu thập link của các danh mục mục tiêu.
    """
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
        browser, context, context_args = await init_browser_context(p)
        logger.info("Pipeline started")

        # cat_links = await collect_category_links(browser, context_args)
        
        # MAX_CONCURRENT_TABS = 3
        # semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
        
        # tasks = [asyncio.create_task(collect_url_product(browser, context_args, cat["name"], cat["url"], semaphore)) for cat in cat_links]
        # await asyncio.gather(*tasks)


        logger.info("Testing with 1 URL mode")
        test_url = "https://www.tiktok.com/shop/vn/pdp/ma-y-massage-ca-m-tay-mini-ma-t-xa-4-%C4%91a-u-6-che-%C4%91o-ai-cam-bien-luc/1731145672550746972"
        await process_product_url(browser, context_args, test_url)
        
        await context.close()
        await browser.close()
