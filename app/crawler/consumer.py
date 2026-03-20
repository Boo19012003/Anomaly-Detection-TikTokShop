import asyncio
import re
import json
from app.crawler.browser import check_captcha_visible, solve_captcha_async, intercept_route
from app.config.settings import get_logger

logger = get_logger("Consumer")

async def select_star_filter(page, star_value: str):
    try:
        filter_container = page.locator('.review-filter-star-select-container-MaTvEI')
        await filter_container.click(timeout=2000)
        
        option_to_click = page.locator('.tux-menu-item').get_by_text(star_value, exact=True)
        await option_to_click.click(timeout=2000)

    except Exception as e:
        logger.error(f"Error selecting star filter: {e}")

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

            elif "/shop/vn/" in resp_url:
                html_text = await response.text()
                match = re.search(r'<script[^>]*id="__MODERN_ROUTER_DATA__"[^>]*>(.*?)</script>', html_text, flags=re.IGNORECASE | re.DOTALL)
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
        
        try:
            sort_container = page.locator('div.flex-col:has-text("Sort by")').first
            dropdown_btn = sort_container.locator('[data-testid="tux-web-select"]').first
            
            await dropdown_btn.click()
            
            option_to_click = page.locator('.tux-menu-item').get_by_text("Most recent", exact=True)
            await option_to_click.click(timeout=2000)

        except Exception as e:
            logger.error(f"Error sorting reviews: {e}")

        for s in ["5", "4", "3", "2", "1"]:
            try:
                valid_reviews_count = 0

                if await check_captcha_visible(page):
                    await solve_captcha_async(page)
                    await page.wait_for_timeout(2000)
                
                # We need to wait for the first response after clicking a star filter
                try:
                    async with page.expect_response(lambda r: "get_product_reviews" in r.url and r.status == 200, timeout=10000) as first_res_info:
                        await select_star_filter(page, s)
                    
                    first_res = await first_res_info.value
                    json_data = await first_res.json()
                    reviews_list = json_data.get("data", {}).get("product_reviews", [])
                    for rev in reviews_list:
                        text = rev.get("review_text", "")
                        if len(text.split()) > 10:
                            valid_reviews_count += 1
                except Exception as e:
                    logger.error(f"Error parsing initial reviews for star {s}: {e}")
                    # fallback in case it times out, though we still try to paginate
                    await select_star_filter(page, s)

                next_button = page.locator('div.flex.items-center:has(div.Headline-Semibold:text-is("Next"))')
                while valid_reviews_count < 30:
                    if not await next_button.is_visible():
                        break
                    class_attribute = await next_button.get_attribute('class')
                    if class_attribute and 'text-color-UITextPlaceholder' in class_attribute:
                        break
                    
                    if await check_captcha_visible(page):
                        await solve_captcha_async(page)
                        await page.wait_for_timeout(2000)
                        
                    try:
                        async with page.expect_response(lambda r: "get_product_reviews" in r.url and r.status == 200, timeout=10000) as response_info:
                            await next_button.click(timeout=2000)
                        
                        # Parse the response to count valid reviews
                        response = await response_info.value
                        try:
                            json_data = await response.json()
                            reviews_list = json_data.get("data", {}).get("product_reviews", [])
                            for rev in reviews_list:
                                text = rev.get("review_text", "")
                                if len(text.split()) > 3:
                                    valid_reviews_count += 1
                        except Exception as e:
                            logger.error(f"Error parsing reviews response: {e}")
                            
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
                logger.error(f"Error processing star {s}: {e}")

    except Exception as e:
        logger.error(f"Error processing URL {url}: {e}")
    
    await page.close()
    await context.close()
    
    return temp_raw_data
