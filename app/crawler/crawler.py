import asyncio
import re
import json
from playwright.async_api import TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
from app.crawler.browser import check_captcha_visible, solve_captcha_async, intercept_route
from app.config.settings import get_logger

logger = get_logger("Crawler")

# Crawl data product
async def crawl_data_product(context, url, semaphore):
    async with semaphore:
        page = await context.new_page()
        
        temp_raw_data = []

        async def handle_response(response):
            if response.request.method == "OPTIONS" or response.status != 200: return
            resp_url = response.url
            try:
                if "/shop/vn/" in resp_url:
                    html_text = await response.text()
                    match = re.search(r'<script[^>]*id="__MODERN_ROUTER_DATA__"[^>]*>(.*?)</script>', html_text, flags=re.IGNORECASE | re.DOTALL)
                    if match:
                        loader_data = json.loads(match.group(1))
                        temp_raw_data.append({"url": resp_url, "type": "html_loader_data", "data": loader_data})
            except (json.JSONDecodeError, UnicodeDecodeError, KeyError) as e:
                logger.error(f"Data mapping error in URL {resp_url}: {e}")
            except Exception as e:
                logger.error(f"Unexpected response error for {resp_url}: {e}")

        page.on("response", handle_response)

        try:
            await page.goto(url, timeout=20000)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except PlaywrightTimeoutError: 
                pass

            if await check_captcha_visible(page):
                await solve_captcha_async(page)

            if not temp_raw_data:
                await page.reload(timeout=20000)
                await page.wait_for_load_state("networkidle", timeout=10000)

        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout loading product URL {url}: {e}")
        except PlaywrightError as e:
            logger.error(f"Playwright error loading product URL {url}: {e}")
        except Exception as e:
            logger.error(f"Error loading {url}: {e}")
        finally:
            await page.close()
        
    return temp_raw_data



# Select star filter
async def select_star_filter(page, star_value: str):
    try:
        filter_container = page.locator('.review-filter-star-select-container-MaTvEI')
        dropdown_btn = filter_container.locator('[data-testid="tux-web-select"]').first
        
        await dropdown_btn.wait_for(state="visible", timeout=5000)
        await dropdown_btn.click(timeout=3000, force=True)
        
        await page.wait_for_timeout(800)
        
        option_to_click = page.locator('.tux-menu-item .Headline-Semibold').filter(has_text=re.compile(f"^{star_value}")).first
        
        # Nếu chưa thấy menu mở ra, thử click mở lại lần nữa
        if not await option_to_click.is_visible():
            await dropdown_btn.click(timeout=2000, force=True)
            await page.wait_for_timeout(800)

        await option_to_click.wait_for(state="visible", timeout=3000)
        await option_to_click.click(timeout=2000, force=True)
        
        await page.wait_for_timeout(1000)

    except PlaywrightTimeoutError as e:
        logger.error(f"Timeout selecting star filter {star_value}: {e}")
        raise e
    except PlaywrightError as e:
        logger.error(f"Playwright error selecting star filter {star_value}: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error selecting star filter {star_value}: {e}")
        raise e

# Crawl data review
async def crawl_data_review(context, url, semaphore):
    async with semaphore:
        page = await context.new_page()
        temp_raw_data = []

        async def handle_response(response):
            if response.request.method == "OPTIONS" or response.status != 200: return
            resp_url = response.url
            resource_type = response.request.resource_type
            try:
                if resource_type in ["fetch"] and "get_product_reviews" in resp_url:
                    json_data = await response.json()
                    
                    reviews_list = json_data.get("data", {}).get("product_reviews", [])
                    if reviews_list:
                        valid_reviews = []
                        for rev in reviews_list:
                            text = rev.get("review_text") or ""
                            if len(text.split()) > 2:
                                valid_reviews.append(rev)
                        
                        if valid_reviews:
                            json_data["data"]["product_reviews"] = valid_reviews
                            temp_raw_data.append({"url": resp_url, "type": "json", "data": json_data})
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                logger.error(f"Data formatting error parsing review response: {e}")
            except Exception as e:
                logger.error(f"Unexpected response error for review {resp_url}: {e}")

        page.on("response", handle_response)

        try:
            await page.goto(url, timeout=20000)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except PlaywrightTimeoutError: 
                pass
                
            if await check_captcha_visible(page):
                await solve_captcha_async(page)

            await page.wait_for_timeout(500)
            
            try:
                dropdown_btn = page.locator('[data-testid="tux-web-select"]').filter(has_text="Recommended").first
                await dropdown_btn.click(timeout=5000, force=True)

                await page.wait_for_timeout(500)

                option_to_click = page.locator('.tux-menu-item').filter(has_text=re.compile(r"Most recent")).first

                await option_to_click.wait_for(state="visible", timeout=5000)
                await option_to_click.click(timeout=5000, force=True)

                await page.wait_for_timeout(500)

            except PlaywrightTimeoutError as e:
                logger.warning(f"Timeout setting initial review sort: {e}")
                try:
                    if await check_captcha_visible(page):
                        await solve_captcha_async(page)
                        await page.wait_for_timeout(2000)
                except PlaywrightError as solve_e:
                    logger.error(f"Playwright error checking captcha in sort fallback: {solve_e}")
            except PlaywrightError as e:
                logger.error(f"Playwright error setting initial review sort: {e}")
            except Exception as e:
                logger.error(f"Unexpected error sorting reviews: {e}")

            for s in ["5", "4", "3", "2", "1"]:
                try:
                    valid_reviews_count = 0
                    filter_success = False

                    for attempt in range(3):
                        if await check_captcha_visible(page):
                            await solve_captcha_async(page)
                            await page.wait_for_timeout(2000)
                        
                        try:
                            async with page.expect_response(lambda r: "get_product_reviews" in r.url and r.status == 200, timeout=5000) as first_res_info:
                                await select_star_filter(page, s)
                            
                            first_res = await first_res_info.value
                            json_data = await first_res.json()
                            reviews_list = json_data.get("data", {}).get("product_reviews", [])
                            for rev in reviews_list:
                                text = rev.get("review_text") or ""
                                if len(text.split()) > 2:
                                    valid_reviews_count += 1
                            filter_success = True
                            break
                        except PlaywrightTimeoutError as e:
                            logger.warning(f"Timeout selecting star filter {s} on attempt {attempt}: {e}")
                        except PlaywrightError as e:
                            logger.error(f"Playwright error selecting star filter {s}: {e}")
                        except Exception as e:
                            logger.error(f"Unexpected error selecting star filter {s}: {e}")
                    if not filter_success:
                        logger.warning(f"Skip next page for star {s} because filter cannot be activated.")
                        continue

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
                            
                            response = await response_info.value
                            try:
                                json_data = await response.json()
                                reviews_list = json_data.get("data", {}).get("product_reviews", [])
                                for rev in reviews_list:
                                    text = rev.get("review_text") or ""
                                    if len(text.split()) > 2:
                                        valid_reviews_count += 1
                            except (json.JSONDecodeError, TypeError, KeyError) as e:
                                logger.error(f"Data formatting error parsing reviews response: {e}")
                            except Exception as e:
                                logger.error(f"Unexpected error parsing reviews response: {e}")
                                
                        except PlaywrightTimeoutError as e:
                            logger.warning(f"Timeout clicking next page for reviews: {e}")
                            if await check_captcha_visible(page):
                                await solve_captcha_async(page)
                                await page.wait_for_timeout(2000)
                                continue
                            break
                        except PlaywrightError as e:
                            logger.warning(f"Playwright error clicking next page: {e}")
                            if await check_captcha_visible(page):
                                await solve_captcha_async(page)
                                await page.wait_for_timeout(2000)
                                continue
                            try:
                                await next_button.scroll_into_view_if_needed()
                                await next_button.click(timeout=2000, force=True)
                                await page.wait_for_timeout(1000)
                            except (PlaywrightTimeoutError, PlaywrightError): 
                                break
                        except Exception as e:
                            logger.error(f"Unexpected error when attempting to click Next: {e}")
                            break

                except PlaywrightError as e:
                    logger.error(f"Playwright error processing star {s}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error processing star {s}: {e}")

        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout loading review URL {url}: {e}")
        except PlaywrightError as e:
            logger.error(f"Playwright error loading review URL {url}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error loading review URL {url}: {e}")
        finally:
            await page.close()
            
        return temp_raw_data

        