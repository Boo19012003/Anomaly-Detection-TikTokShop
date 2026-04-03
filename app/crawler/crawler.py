import asyncio
import re
import json
from playwright.async_api import TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
from app.crawler.browser import handle_captcha
from app.config.settings import get_logger

logger = get_logger("Crawler")

async def crawl_data_product(context, url, semaphore):
    async with semaphore:
        page = await context.new_page()
        temp_raw_data = []

        try:
            await page.goto(url, timeout=20000)
            script_locator = page.locator('#__MODERN_ROUTER_DATA__')
            
            try:
                await script_locator.wait_for(state="attached", timeout=5000)
                
            except PlaywrightTimeoutError:
                await handle_captcha(page)
                
                try:
                    await page.reload(timeout=15000)
                    await script_locator.wait_for(state="attached", timeout=10000)
                except PlaywrightTimeoutError:
                    logger.warning(f"Vẫn không tìm thấy thẻ data cho URL {url} sau khi đã giải captcha và reload")
                    return temp_raw_data

            json_text = await script_locator.inner_text()
            if json_text:
                loader_data = json.loads(json_text)
                temp_raw_data.append({
                    "url": page.url, 
                    "type": "html_loader_data", 
                    "data": loader_data
                })

        except Exception as e:
            logger.error(f"Lỗi không xác định khi xử lý {url}: {e}")
        finally:
            await page.close()
            
    return temp_raw_data

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
                            if len(text.split()) >= 3:
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
                await page.wait_for_response("**/get_product_review*", timeout=5000)
            except PlaywrightTimeoutError: 
                pass
                
            await handle_captcha(page)

            await page.wait_for_timeout(300)
            
            try:
                review_section = page.locator('#pdp-review-section').first
                if await review_section.is_visible():
                    await review_section.scroll_into_view_if_needed(timeout=3000)

                dropdown_btn = page.locator('[data-testid="tux-web-select"]').filter(has_text="Recommended").first
                if not await dropdown_btn.is_visible():
                    dropdown_btn = page.locator('div').filter(has_text=re.compile(r"^Sort by$")).locator('..').locator('[data-testid="tux-web-select"]').first
                
                await dropdown_btn.scroll_into_view_if_needed(timeout=3000)
                await dropdown_btn.click(timeout=3000, force=True)

                await page.wait_for_timeout(500)

                option_to_click = page.locator('.tux-menu-item').filter(has_text=re.compile(r"Most recent", re.IGNORECASE)).first

                if not await option_to_click.is_visible():
                    await dropdown_btn.click(timeout=2000, force=True)
                    await page.wait_for_timeout(500)

                await option_to_click.wait_for(state="visible", timeout=3000)
                await option_to_click.click(timeout=3000, force=True)

                await page.wait_for_timeout(500)

            except PlaywrightTimeoutError as e:
                logger.warning(f"Timeout setting initial review sort: {e}")
                await handle_captcha(page, wait_ms=1000)
            except PlaywrightError as e:
                logger.error(f"Playwright error setting initial review sort: {e}")
            except Exception as e:
                logger.error(f"Unexpected error sorting reviews: {e}")

            next_button = page.locator('div.flex.items-center:has(div.Headline-Semibold:text-is("Next"))')
            valid_reviews_count = 0
            
            async def is_next_disabled():
                if not await next_button.is_visible():
                    return True
                cls = await next_button.get_attribute('class') or ""
                return 'text-color-UITextPlaceholder' in cls
            
            while valid_reviews_count < 100:
                if await is_next_disabled():
                    logger.info(f"No more pages, collected {valid_reviews_count} valid reviews.")
                    break
                
                await handle_captcha(page, wait_ms=1000)
                
                if await is_next_disabled():
                    logger.info(f"No more pages, collected {valid_reviews_count} valid reviews.")
                    break
                    
                try:
                    async with page.expect_response(lambda r: "get_product_reviews" in r.url and r.status == 200, timeout=5000) as response_info:
                        await next_button.click(timeout=2000)
                    
                    response = await response_info.value
                    try:
                        json_data = await response.json()
                        reviews_list = json_data.get("data", {}).get("product_reviews", [])
                        for rev in reviews_list:
                            text = rev.get("review_text") or ""
                            if len(text.split()) >= 3:
                                valid_reviews_count += 1
                    except (json.JSONDecodeError, TypeError, KeyError) as e:
                        logger.error(f"Data formatting error parsing reviews response: {e}")
                    except Exception as e:
                        logger.error(f"Unexpected error parsing reviews response: {e}")
                        
                except PlaywrightTimeoutError:
                    if await is_next_disabled():
                        logger.info(f"Reached last page, collected {valid_reviews_count} valid reviews.")
                        break
                    if await handle_captcha(page, wait_ms=1000):
                        continue
                    logger.warning("Timeout clicking next page, skipping.")
                    break
                except PlaywrightError as e:
                    logger.warning(f"Playwright error clicking next page: {e}")
                    if await handle_captcha(page, wait_ms=1000):
                        continue
                    break
                except Exception as e:
                    logger.error(f"Unexpected error when attempting to click Next: {e}")
                    break

        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout loading review URL {url}: {e}")
        except PlaywrightError as e:
            logger.error(f"Playwright error loading review URL {url}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error loading review URL {url}: {e}")
        finally:
            await page.close()
            
        return temp_raw_data