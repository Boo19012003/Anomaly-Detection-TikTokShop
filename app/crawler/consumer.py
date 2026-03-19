import asyncio
import re
import json
from app.crawler.browser import check_captcha_visible, solve_captcha_async, intercept_route
from app.config.settings import get_logger

logger = get_logger("Consumer")

async def crarwl_data_product(browser, context_args, url):
    context = await browser.new_context(**context_args)
    await context.route("**/*", intercept_route)
    page = await context.new_page()
    
    temp_raw_data = []

    async def handle_response(response):
        if response.request.method == "OPTIONS":
            return
        if response.status != 200:
            return

        resp_url = response.url
        resource_type = response.request.resource_type

        try:
            if resource_type in ["fetch", "xhr"] and ("get_product" in resp_url or "api/v1" in resp_url):
                json_data = await response.json()
                temp_raw_data.append({
                    "url": resp_url,
                    "type": "api_json", 
                    "data": json_data
                })

            elif resource_type == "document" and "/shop/vn/" in resp_url:
                html_text = await response.text()
                match = re.search(r'<script[^>]*id="pumbaa-rule"[^>]*>(.*?)</script>', html_text, re.IGNORECASE)
                if match:
                    try:
                        loader_data = json.loads(match.group(1))
                        temp_raw_data.append({
                            "url": resp_url,
                            "type": "html_loader_data",
                            "data": loader_data
                        })
                    except json.JSONDecodeError:
                        logger.error(f"Parse JSON error tracking: {resp_url}")
        except Exception as e:
            logger.error(f"Response error from {resp_url[:100]}: {e}")

    page.on("response", handle_response)

    try:
        await page.goto(url, timeout=20000)
        try:
            await page.wait_for_load_state("networkidle", timeout=5000)
        except: pass
            
        if await check_captcha_visible(page):
            await solve_captcha_async(page)

        next_button = page.locator('div.flex.items-center:has(div.Headline-Semibold:text-is("Next"))')

        while True:
            if not await next_button.is_visible():
                break
            class_attribute = await next_button.get_attribute('class')
            if class_attribute and 'text-color-UITextPlaceholder' in class_attribute:
                break
            
            if await check_captcha_visible(page):
                await solve_captcha_async(page)
                await page.wait_for_timeout(2000)
                
            try:
                async with page.expect_response(lambda r: "get_product_reviews" in r.url and r.status == 200, timeout=10000):
                    await next_button.click(timeout=2000)
            except Exception as e:
                if "Timeout" in str(e): break
            except Exception:
                if await check_captcha_visible(page):
                    await solve_captcha_async(page)
                    await page.wait_for_timeout(1000)
                    continue
                try:
                    await next_button.scroll_into_view_if_needed()
                    await next_button.click(timeout=2000, force=True)
                    await page.wait_for_timeout(1000)
                except: break

    except Exception as e:
        logger.error(f"Error processing URL {url}: {e}")
    
    await page.close()
    await context.close()
    
    return temp_raw_data
