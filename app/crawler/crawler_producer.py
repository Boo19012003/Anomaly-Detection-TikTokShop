from asyncio import timeout
import asyncio
import os
import sys
import json
import logging
import random
import re
from datetime import datetime, timezone

from playwright.async_api import async_playwright
from dotenv import load_dotenv

# Import module giải captcha async
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from captcha_solver import solve_tiktok_captcha

# ==========================================
# Configure LOGGING
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CrawlerProducer")

# Turn off INFO logs (HTTP Request, etc.) but keep WARNING and ERROR
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("ultralytics").setLevel(logging.WARNING)
logging.getLogger("playwright").setLevel(logging.WARNING)

load_dotenv()

# ==========================================
# Utility Functions
# ==========================================
async def solve_captcha_async(page):
    """
    Loop to call AI Model to solve captcha
    """
    captcha_count = 0
    max_retries = 5
    while captcha_count < max_retries:
        try:
            status = await solve_tiktok_captcha(page)
        except Exception as e:
            logger.warning(f"Captcha module error: {e}")
            break

        if status == "no_captcha":
            break
        elif status == "success":
            await asyncio.sleep(0.5)
            break
        else:
            captcha_count += 1
            logger.warning(f"Retry {captcha_count}/{max_retries}")
            await asyncio.sleep(1)

async def intercept_route(route):
    """
    Block load image/font, only allow API & HTML
    """
    url = route.request.url.lower()
    if "ibyteimg.com" in url and ("-origin-jpeg.jpeg" in url or "-origin-png.png" in url):
        await route.continue_()
        return

    if route.request.resource_type in ["image", "media", "font"]:
        await route.abort()
    else:
        await route.continue_()

async def check_captcha_visible(page):
    """
    Check if captcha is visible
    """
    selectors = "#captcha_container, .captcha_verify_container, #tts_web_captcha_container"
    captcha_locators = page.locator(selectors)
    
    for i in range(await captcha_locators.count()):
        if await captcha_locators.nth(i).is_visible():
            return True
    return False

# ==========================================
# DATA COLLECTION LOGIC
# ==========================================
async def collect_url_product(browser, context_args, name, url, semaphore):
    """
    Collect product URLs from category page
    """
    async with semaphore:
        context = await browser.new_context(**context_args)
        await context.route("**/*", intercept_route)
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
                    cat_links_list.append(href)

            logger.info(f"{name} collected {len(cat_links_list)} links")
            return cat_links_list

        except Exception as e:
            logger.error(f"{name} error: {e}")
        finally:
            await page.close()
            await context.close()

async def crarwl_data_product(browser, context_args, url):
    """
    Crawl data from product page
    """
    
    context = await browser.new_context(**context_args)
    await context.route("**/*", intercept_route)
    page = await context.new_page()
    
    temp_raw_data = []

    async def handle_response(response):
        if response.request.method == "OPTIONS":
            return

        if response.status != 200:
            return

        url = response.url
        resource_type = response.request.resource_type

        try:
            if resource_type in ["fetch", "xhr"] and ("get_product" in url or "api/v1" in url):
                json_data = await response.json()
                
                temp_raw_data.append({
                    "url": url,
                    "type": "api_json", 
                    "data": json_data
                })

            elif resource_type == "document" and "/shop/vn/" in url:
                html_text = await response.text()
                
                match = re.search(r'<script[^>]*id="pumbaa-rule"[^>]*>(.*?)</script>', html_text, re.IGNORECASE)
                
                if match:
                    raw_json_str = match.group(1)
                    try:
                        loader_data = json.loads(raw_json_str)
                        temp_raw_data.append({
                            "url": url,
                            "type": "html_loader_data",
                            "data": loader_data
                        })
                    except json.JSONDecodeError:
                        logger.error(f"Parse JSON from the extracted string in HTML: {url}")
                else:
                    logger.warning(f"Script containing loaderData not found: {url}")

        except Exception as e:
            logger.error(f"Error capturing response from {url[:100]}: {e}")

    page.on("response", handle_response)

    try:
        await page.goto(url, timeout=20000)
        
        try:
            await page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
            
        if await check_captcha_visible(page):
            await solve_captcha_async(page)

        next_button = page.locator('div.flex.items-center:has(div.Headline-Semibold:text-is("Next"))')

        while True:
            if not await next_button.is_visible():
                break
                
            class_attribute = await next_button.get_attribute('class')
            if class_attribute and 'text-color-UITextPlaceholder' in class_attribute:
                logger.debug('Reached the last page of reviews.')
                break
            
            if await check_captcha_visible(page):
                await solve_captcha_async(page)
                await page.wait_for_timeout(2000)
                
            try:
                async with page.expect_response(lambda r: "get_product_reviews" in r.url and r.status == 200, timeout=10000):
                    await next_button.click(timeout=2000)
                    
            except Exception as e:
                if "Timeout" in str(e):
                    logger.debug("No review API response.")
                    break
                    
            except Exception:
                if await check_captcha_visible(page):
                    await solve_captcha_async(page)
                    await page.wait_for_timeout(1000)
                    continue
                
                try:
                    await next_button.scroll_into_view_if_needed()
                    await next_button.click(timeout=2000, force=True)
                    await page.wait_for_timeout(1000)
                except Exception as e:
                    logger.warning(f"Cannot next to page: {e}")
                    break

    except Exception as e:
        logger.error(f"Error url {url}: {e}")
    
    await page.close()
    await context.close()
    
    return temp_raw_data

async def crawler():
    """
    Main crawler - Collect data from TikTok Shop
    """
    async with async_playwright() as p:
        args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-infobars',
            '--no-sandbox'
        ]

        logger.info("Open browser")
        launch_args = dict(
            headless= False,
            channel="chrome",
            args=args,
        )

        browser = await p.chromium.launch(**launch_args)
        state_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "auth", "state.json"))
        
        USER_AGENTS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
        
        context_args = {
            "viewport": {"width": 1280, "height": 720},
            "user_agent": random.choice(USER_AGENTS)
        }
        
        if os.path.exists(state_path):
            context_args["storage_state"] = state_path
        else:
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            logger.warning(f"Not found storage_state at: {state_path}")

        context = await browser.new_context(**context_args)
        await context.route("**/*", intercept_route)
        
        # home_page = await context.new_page()
        # await home_page.goto("https://www.tiktok.com/shop/vn", timeout=30000)
        
        # """
        # Solving captcha
        # """
        # try:
        #     await home_page.wait_for_load_state("networkidle", timeout=5000)
        # except Exception:
        #     pass
        # if await check_captcha_visible(home_page):
        #     await solve_captcha_async(home_page)

        # """
        # Take category links
        # """
        # try:
        #     await home_page.wait_for_selector('a[href*="/c/"]', timeout=10000)
        # except Exception:
        #     logger.warning("Not found category links")

        # target_categories = [
        #     "Womenswear & Underwear", 
        #     "Menswear & Underwear", 
        #     "Beauty & Personal Care"
        # ]
        # cat_links = []
        
        # cat_elements = await home_page.query_selector_all('a[href*="/c/"]')
        # found_names = []
        
        # for cat in cat_elements:
        #     name = (await cat.inner_text()).strip()
        #     if name:
        #         found_names.append(name)
                
        #     if name in target_categories:
        #         url = await cat.get_attribute('href')
        #         full_url = "https://www.tiktok.com" + url if url and url.startswith("/c/") else url
        #         cat_links.append({"name": name, "url": full_url})
        

        # logger.info(f"Taked {len(cat_links)} target categories.")

        # for cat in cat_links:
        #     logger.info(f"Crawling category: {cat['name']} - {cat['url']}")

        # await home_page.close()
        
        # """
        # Take product links
        # """
        # MAX_CONCURRENT_TABS = 3
        # semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)

        # tasks = [asyncio.create_task(collect_url_product(browser, context_args, cat["name"], cat["url"], semaphore)) for cat in cat_links]
        # results = await asyncio.gather(*tasks)

        # url_list = []

        # for links in results:
        #     url_list.extend(links)

        test_url = "https://www.tiktok.com/shop/vn/pdp/dau-goi-thao-duoc-namnung-chiet-xuat-thien-nhien-185ml/1730089185693370633"
        logger.info(f"Start crawling 1 test product...")
        
        data = await crarwl_data_product(browser, context_args, test_url)
        logger.info(f"Crawled {len(data)} items from test URL.")
        print(data) # In ra nếu cần thiết

        await context.close()
        await browser.close()
        
        logger.info("Crawler completed!")

if __name__ == "__main__":
    asyncio.run(crawler())
