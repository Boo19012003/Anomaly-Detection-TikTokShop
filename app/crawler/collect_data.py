import asyncio
import os
import sys
import json
import re
import urllib.parse
import logging
import random
from datetime import datetime, timezone
from math import pow


from playwright.async_api import async_playwright
from supabase import create_client, Client
from dotenv import load_dotenv

# Import module giải captcha async
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from captcha_solver import solve_tiktok_captcha

# ==========================================
# CẤU HÌNH LOGGING & DATABASE SUPABASE
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataCollector")

# Tắt các dòng log INFO (HTTP Request, v.v.) nhưng vẫn giữ lại WARNING và ERROR
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Tắt các dòng thông báo từ YOLO (Ultralytics)
logging.getLogger("ultralytics").setLevel(logging.WARNING)

# Nếu bạn muốn tắt cả log từ Playwright
logging.getLogger("playwright").setLevel(logging.WARNING)

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if SUPABASE_URL and SUPABASE_KEY:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
else:
    logger.warning("Thiếu biến môi trường Supabase! Các thao tác DB sẽ bị bỏ qua.")

# ==========================================
# CÁC HÀM TIỆN ÍCH & TỐI ƯU HÓA CPU
# ==========================================
async def solve_captcha_async(page):
    """Vòng lặp gọi AI Model để giải captcha liên tục nếu xuất hiện"""
    captcha_count = 0
    max_retries = 5
    while captcha_count < max_retries:
        try:
            status = await solve_tiktok_captcha(page)
        except Exception as e:
            logger.warning(f"Lỗi module Captcha: {e}")
            break

        if status == "no_captcha":
            break
        elif status == "success":
            await asyncio.sleep(0.5)
            break
        else:
            captcha_count += 1
            logger.warning(f"[Solve Captcha] Thử lại lần {captcha_count}/{max_retries}")
            await asyncio.sleep(1)

async def calculate_weight_time(review_date: datetime):
    now = datetime.now(timezone.utc)
    delta_t = (now - review_date).days
    return pow(0.5, delta_t / 30.0)

async def intercept_route(route):
    url = route.request.url.lower()
    if "ibyteimg.com" in url and ("-origin-jpeg.jpeg" in url or "-origin-png.png" in url):
        await route.continue_()
        return

    if route.request.resource_type in ["image", "media", "font"]:
        await route.abort()
    else:
        await route.continue_()

async def check_captcha_visible(page):
    """Kiểm tra xem có bất kỳ khung Captcha nào đang hiển thị trên màn hình không (tránh lỗi Strict Mode)"""
    selectors = "#captcha_container, .captcha_verify_container, #tts_web_captcha_container"
    captcha_locators = page.locator(selectors)
    
    for i in range(await captcha_locators.count()):
        if await captcha_locators.nth(i).is_visible():
            return True
    return False

def extract_json_from_html_sync(text_data, url):
    """
    Hàm đồng bộ chứa các tác vụ Regex nặng. 
    Sẽ được chạy trên ThreadPool để không block Event Loop của Playwright.
    """
    product_data_chunk = []
    
    # Tìm script application/json
    json_matches = re.finditer(r'<script[^>]*type=["\']application/json["\'][^>]*>([\s\S]*?)</script>', text_data)
    for match in json_matches:
        try:
            json_content = match.group(1).strip()
            if json_content:
                json_data = json.loads(json_content)
                product_data_chunk.append({
                    "url": url,
                    "type": "text_json",
                    "data": json_data
                })
        except json.JSONDecodeError:
            pass
            
    # Tìm Universal Data
    unv_mach = re.search(r'<script[^>]*id=["\']__UNIVERSAL_DATA_FOR_REHYDRATION__["\'][^>]*>([\s\S]*?)</script>', text_data)
    if unv_mach:
        try:
            json_data = json.loads(unv_mach.group(1).strip())
            product_data_chunk.append({
                "url": url,
                "type": "universal_data",
                "data": json_data
            })
        except json.JSONDecodeError:
            pass
            
    return product_data_chunk

async def parse_tiktok_description(raw_desc):
    if not raw_desc:
        return ""
    try:
        desc_blocks = json.loads(raw_desc)
        texts = [block.get("text", "").strip() for block in desc_blocks if block.get("type") == "text" and block.get("text", "").strip()]
        return " ".join(texts)
    except json.JSONDecodeError:
        return str(raw_desc).strip()

async def parse_number(text):
    if text is None or str(text).strip() == "":
        return 0
    if isinstance(text, (int, float)):
        return text

    text = str(text).strip().upper().replace(",", "").replace("%", "")
    multiplier = 1
    if text.endswith("K"):
        multiplier = 1000
        text = text[:-1]
    elif text.endswith("M"):
        multiplier = 1000000
        text = text[:-1]
        
    try:
        value = float(text) * multiplier
        if value.is_integer():
            return int(value)
        return value
    except ValueError:
        return 0

# ==========================================
# DATABASE UPSERT LOGIC
# ==========================================
async def upsert_to_supabase(structured_data):
    """
    Đẩy dữ liệu đã làm sạch lên Database.
    Hàm này được thiết kế để Cập nhật (Upsert) dựa trên ID để tránh duplicate data 
    khi tracking sự thay đổi của thị trường.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
        
    try:
        # Upsert Shops
        if structured_data.get("shops"):
            supabase.table("shops").upsert(structured_data["shops"]).execute()
            
        # Upsert Products
        if structured_data.get("products"):
            supabase.table("products").upsert(structured_data["products"]).execute()

        # Insert/Upsert Product Metrics History
        if structured_data.get("products_metrics_history"):
            supabase.table("products_metrics_history").insert(structured_data["products_metrics_history"]).execute()
            
        # Upsert Reviews
        if structured_data.get("reviews"):
            # Vì số lượng review có thể lớn, nên chia lô (chunk) nếu cần thiết khi gọi DB thật
            supabase.table("reviews").upsert(structured_data["reviews"]).execute()

    except Exception as e:
        logger.error(f"Lỗi khi đẩy dữ liệu lên Supabase: {e}")

# ==========================================
# CÁC HÀM XỬ LÝ DỮ LIỆU
# ==========================================
async def extract_tiktok_data(json_data_list):
    shops = {}
    products = {}
    products_metrics_history = {}
    reviews = {}

    for item in json_data_list:
        url = item.get("url", "")
        payload = item.get("data", {})

        if "pdp" in url:
            logger.debug(f"Đang xử lý dữ liệu từ PDP: {url}")

        # 1. TRÍCH XUẤT TỪ PDP
        if "pdp" in url and isinstance(payload, dict) and "loaderData" in payload:
            loader_data = payload.get("loaderData", {})
            pdp_page = loader_data.get("shop/(region)/pdp/(product_name_slug$)/(product_id)/page", {})
            if not pdp_page:
                continue

            page_config = pdp_page.get("page_config", {})
            components_map = page_config.get("components_map", [])
            
            product_info_block = {}
            component_data = {}
            for comp in components_map:
                if comp.get("component_type") == "product_info":
                    component_data = comp.get("component_data", {})
                    product_info_block = component_data.get("product_info", {})
                    break
            
            if not product_info_block:
                continue

            product_model = product_info_block.get("product_model", {})
            product_detail = product_model.get("product_detail", {})
            seller_model = product_info_block.get("seller_model", {})
            shop_base_info = seller_model.get("shop_base_info", {})
            shop_info = component_data.get("shop_info", {})

            shop_exp_scores = shop_base_info.get("shop_exp_scores", [])
            response_rate = next((score.get("format_score") for score in shop_exp_scores if score.get("score_description") == "24h response rate"), None)

            shop_id = shop_base_info.get("seller_id")
            if shop_id and shop_id not in shops:
                shops[shop_id] = {
                    "shop_id": shop_id,
                    "name": shop_base_info.get("shop_name"),
                    "rating_avg": await parse_number(shop_base_info.get("shop_rating_review")),
                    "response_rate": await parse_number(response_rate if response_rate != "-" else "0"),
                    "followers_count": int(shop_info.get("followers_count", 0)),
                    "is_rising_star": False,
                    "safe_score": 0.0,
                }

            product_id = product_detail.get("product_id")
            if product_id and product_id not in products:
                categories = product_detail.get("categories", [])
                category_name = next((c.get("category_name") for c in categories if c.get("level") == 1), categories[0].get("category_name") if categories else None)

                products[product_id] = {
                    "product_id": product_id,
                    "shop_id": shop_id,
                    "category": category_name,
                    "title": product_detail.get("name"),
                    "description": await parse_tiktok_description(product_detail.get("description")),
                    "product_url": url,
                }

            if product_id and product_id not in products_metrics_history:
                skus = product_detail.get("skus", [])
                total_quantity = sum(sku.get("sku_quantity", {}).get("available_quantity", 0) for sku in skus)
                
                promotion_tag = component_data.get("promotion_tag", {}).get("placement_labels", {})
                deal_text = promotion_tag.get("1", [{}])[0].get("text") if promotion_tag.get("1") else None

                products_metrics_history[product_id] = {
                    "product_id": product_id,
                    "sold_count": await parse_number(product_detail.get("sold", "0")),
                    "review_count": await parse_number(product_info_block.get("review_model", {}).get("product_review_count", "0")),
                    "rating_avg": await parse_number(product_info_block.get("review_model", {}).get("product_overall_score", 0.0)),
                    "quantity": total_quantity,
                    "price": await parse_number(product_info_block.get("promotion_model", {}).get("price_view", {}).get("min_price_item", {}).get("real_price")),
                    "discount_percent": await parse_number(product_info_block.get("promotion_model", {}).get("price_view", {}).get("min_price_item", {}).get("discount")),
                    "is_flash_sale": True if deal_text == "Flash sale" else False,
                    "is_deal": True if deal_text == "Deal" else False,
                    "is_anomaly": False,
                    "anomaly_score": 0.0,
                    "scraped_at": datetime.now(timezone.utc).isoformat() # Đã đổi từ recorded_at -> scraped_at
                }

            review_info = component_data.get("review_info", {})
            for rev in review_info.get("product_reviews", []):
                rev_id = rev.get("review_id")
                if rev_id and rev_id not in reviews:
                    ts_ms = int(rev.get("review_time", 0))
                    dt_obj = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date() if ts_ms else None

                    reviews[rev_id] = {
                        "review_id": rev_id,
                        "product_id": rev.get("product_id"),
                        "sku_id": rev.get("sku_id"),
                        "sku_specification": rev.get("sku_specification"),
                        "rating": rev.get("review_rating"), 
                        "text": await parse_tiktok_description(rev.get("review_text", "")),
                        "is_verified": rev.get("is_verified_purchase", False),
                        "review_time": dt_obj.isoformat() if dt_obj else None # Đã đổi từ review_timestamp -> review_time
                    }

        # 2. TRÍCH XUẤT TỪ CÁC API PHÂN TRANG REVIEWS
        elif "get_product_reviews" in url and isinstance(payload, dict):
            api_data = payload.get("data", {})
            paginated_reviews = api_data.get("product_reviews", [])
            
            if not paginated_reviews:
                continue
                
            for rev in paginated_reviews:
                rev_id = rev.get("review_id")
                if rev_id and rev_id not in reviews:
                    ts_ms = int(rev.get("review_time", 0))
                    dt_obj = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date() if ts_ms else None

                    reviews[rev_id] = {
                        "review_id": rev_id,
                        "product_id": rev.get("product_id"),
                        "sku_id": rev.get("sku_id"),
                        "sku_specification": rev.get("sku_specification"),
                        "rating": rev.get("review_rating"), 
                        "text": await parse_tiktok_description(rev.get("review_text", "")),
                        "is_verified": rev.get("is_verified_purchase", False),
                        "review_time": dt_obj.isoformat() if dt_obj else None # Đã đổi từ review_timestamp -> review_time
                    }
    return {
        "shops": list(shops.values()),
        "products": list(products.values()),
        "products_metrics_history": list(products_metrics_history.values()),
        "reviews": list(reviews.values())
    }

# ==========================================
# LOGIC THU THẬP & ĐIỀU HƯỚNG
# ==========================================
async def collect_url_product(context, name, url, semaphore):
    async with semaphore:
        page = await context.new_page()
        cat_links_list = []
        try:
            await page.goto(url, timeout=10000)
            while True:
                # Kiểm tra Captcha nhẹ nhàng hơn
                if await page.locator(".captcha_verify_container").is_visible():
                    await solve_captcha_async(page)
                else:
                    if await page.locator('div.flex.justify-center.mt-16:has-text("No more products")').is_visible():
                        break
                    else:
                        view_more = page.locator('div.flex.justify-center.mt-16:has-text("View more")')
                        if await view_more.is_visible():
                            await view_more.click()
                            await page.wait_for_timeout(1000)
                        else:
                            break

            await page.mouse.wheel(0, 800)
            await asyncio.sleep(1)

            product_cards = await page.query_selector_all("div[class*='rounded']:has(a[href*='/pdp/'])")
            for card in product_cards:
                link_el = await card.query_selector("a[href*='/pdp/']")
                if link_el:
                    href = await link_el.get_attribute("href")
                    if href:
                        link = "https://www.tiktok.com" + href if href.startswith("/pdp/") else href
                        cat_links_list.append(link)
        except Exception as e:
            logger.error(f"[Category] {name} error: {e}")
        finally:
            await page.close()

        logger.info(f"{name} đã gom được {len(cat_links_list)} links")
        return cat_links_list

async def process_request_product(context, url_list, max_concurrent_tabs):
    final_aggregated_data = {
        "shops": [],
        "products": [],
        "products_metrics_history": [],
        "reviews": []
    }
    
    semaphore = asyncio.Semaphore(max_concurrent_tabs)

    async def process_single_url(url):
        async with semaphore:
            page = await context.new_page()
            
            # Buffer lưu trữ data tạm thời cho riêng 1 URL
            temp_raw_data = []

            async def handle_response(response):
                # Lọc kỹ API để giảm rác lưu vào bộ nhớ
                if ("/shop/vn/" in response.url or "get_product_reviews" in response.url) and response.status == 200:
                    try:
                        content_type = response.headers.get("content-type", "").lower()
                        if "application/json" in content_type:
                            json_data = await response.json()
                            temp_raw_data.append({"url": response.url, "type": "json", "data": json_data})
                        elif "text" in content_type:
                            text_data = await response.text()
                            # Tối ưu: Đẩy tác vụ Regex sang ThreadPool để không block Playwright
                            chunks = await asyncio.to_thread(extract_json_from_html_sync, text_data, response.url)
                            temp_raw_data.extend(chunks)
                    except Exception as e:
                        logger.error(f"Error parsing response: {e}")

            page.on("response", handle_response)

            try:
                await page.goto(url, timeout=20000)
                
                try:
                    await page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    # Kệ lỗi timeout, vì data Universal JSON thường đã tải xong rồi
                    pass
                    
                # Check Captcha ban đầu an toàn
                if await check_captcha_visible(page):
                    await solve_captcha_async(page)

                next_button = page.locator('div.flex.items-center:has(div.Headline-Semibold:text-is("Next"))')

                while True:
                    if await next_button.count() == 0 or not await next_button.is_visible():
                        break
                        
                    class_attribute = await next_button.get_attribute('class')
                    if class_attribute and 'text-color-UITextPlaceholder' in class_attribute:
                        logger.debug('Đã đến trang cuối cùng của review.')
                        break
                    
                    # 1. Kiểm tra an toàn: Có Captcha nào đang lù lù trên màn hình không?
                    if await check_captcha_visible(page):
                        await solve_captcha_async(page)
                        await page.wait_for_timeout(1000) # Chờ popup biến mất hoàn toàn
                        
                    # 2. Bắt đầu bấm Next
                    try:
                        async with page.expect_response(lambda r: "get_product_reviews" in r.url and r.status == 200, timeout=10000):
                            await next_button.click(timeout=2000)
                            
                    except Exception as e:
                        # Nếu chỉ là do timeout API (ví dụ hết review) thì break để lưu data
                        if "Timeout" in str(e):
                            logger.debug("Không thấy API review phản hồi, có thể đã hết đánh giá.")
                            break
                            
                    except Exception:
                        # Nếu click lỗi, kiểm tra lại xem có phải Captcha vừa nhảy ra chặn không
                        if await check_captcha_visible(page):
                            await solve_captcha_async(page)
                            await page.wait_for_timeout(1000)
                            continue # Quay lại đầu vòng lặp để click Next lại
                        
                        # Không phải do Captcha, dùng click ép (force)
                        try:
                            await next_button.scroll_into_view_if_needed()
                            await next_button.click(timeout=2000, force=True)
                            await page.wait_for_timeout(1000)
                        except Exception as e:
                            logger.warning(f"Kẹt nút Next không thể qua trang: {e}")
                            break

            except Exception as e:
                logger.error(f"[Extract Data] Error url {url}: {e}")
            
            await page.close()
            
            # Giải phóng RAM ngay lập tức bằng cách bóc tách data của URL này
            if temp_raw_data:
                extracted = await extract_tiktok_data(temp_raw_data)
                
                # Hàm thực hiện đẩy trực tiếp vào DB (đã comment)
                await upsert_to_supabase(extracted)
                
                return extracted
            return None

    tasks = [asyncio.create_task(process_single_url(url)) for url in url_list]
    results = await asyncio.gather(*tasks)
    
    # Gộp dữ liệu đã bóc tách từ các tác vụ
    for res in results:
        if res:
            final_aggregated_data["shops"].extend(res.get("shops", []))
            final_aggregated_data["products"].extend(res.get("products", []))
            final_aggregated_data["products_metrics_history"].extend(res.get("products_metrics_history", []))
            final_aggregated_data["reviews"].extend(res.get("reviews", []))
        
        else:
            # Nếu kết quả trả về rỗng, in ra URL để kiểm tra thủ công
            logger.error(f"URL thứ {i} không trích xuất được dữ liệu: {url_list[i]}")

    # Xóa duplicates nhẹ nhàng trước khi lưu file JSON cho gọn
    final_aggregated_data["shops"] = list({s["shop_id"]: s for s in final_aggregated_data["shops"]}.values())
    final_aggregated_data["products"] = list({p["product_id"]: p for p in final_aggregated_data["products"]}.values())
    final_aggregated_data["reviews"] = list({r["review_id"]: r for r in final_aggregated_data["reviews"]}.values())

    return final_aggregated_data

async def crawler():
    async with async_playwright() as p:
        args = [
            '--disable-blink-features=AutomationControlled',
            '--window-size=1280,720',
            '--disable-infobars',
            '--no-sandbox'
        ]

        logger.info("Mở trình duyệt Crawl...")
        proxy_server = os.getenv("PROXY_SERVER")  # vd: "http://user:pass@proxy-host:port"
        launch_args = dict(
            headless=os.getenv("HEADLESS", "true").lower() != "false",
            channel="chrome",
            args=args,
        )
        if proxy_server:
            launch_args["proxy"] = {"server": proxy_server}
            logger.info(f"Dùng proxy: {proxy_server.split('@')[-1]}")  # ẩn user:pass

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
            logger.warning(f"Không tìm thấy storage_state tại: {state_path}")

        context = await browser.new_context(**context_args)
        await context.route("**/*", intercept_route)
        
        home_page = await context.new_page()
        await home_page.goto("https://www.tiktok.com/shop/vn", timeout=30000)
        
        # 1. Kiểm tra và giải Captcha
        try:
            await home_page.locator(".captcha_verify_container").wait_for(state="visible", timeout=3000)
            await solve_captcha_async(home_page)
            # QUAN TRỌNG: Chờ một chút để TikTok tải lại giao diện sau khi qua Captcha
            await asyncio.sleep(3) 
        except Exception:
            pass
        
        # 2. Chờ cho đến khi các thẻ link danh mục (/c/) thực sự xuất hiện
        logger.info("Đang chờ tải danh sách danh mục (Categories)...")
        try:
            await home_page.wait_for_selector('a[href*="/c/"]', timeout=10000)
        except Exception:
            logger.warning("Không tìm thấy danh mục sau 10s. Có thể mạng chậm hoặc giao diện bị đổi.")

        # Cuộn trang một chút đề phòng lazy-load
        await home_page.mouse.wheel(0, 500)
        await asyncio.sleep(2)
        
        target_categories = [
            "Womenswear & Underwear", 
            "Menswear & Underwear", 
            "Beauty & Personal Care"
        ]
        cat_links = []
        
        cat_elements = await home_page.query_selector_all('a[href*="/c/"]')
        found_names = [] # Lưu tạm để debug
        
        for cat in cat_elements:
            name = (await cat.inner_text()).strip()
            if name:
                found_names.append(name)
                
            if name in target_categories:
                url = await cat.get_attribute('href')
                full_url = "https://www.tiktok.com" + url if url and url.startswith("/c/") else url
                cat_links.append({"name": name, "url": full_url})
        
        # 3. Log kiểm tra nếu mảng vẫn rỗng
        if not cat_links:
            # Lọc trùng lặp danh sách tên để dễ nhìn
            unique_names = list(set(found_names))
            logger.error(f"LỖI: Không map được danh mục nào! Các danh mục đang hiển thị trên web: {unique_names}")
            await context.close()
            return
            
        logger.info(f"Đã lấy được {len(cat_links)} danh mục mục tiêu.")
        
        MAX_CONCURRENT_TABS = 3
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)

        tasks = [asyncio.create_task(collect_url_product(context, cat["name"], cat["url"], semaphore)) for cat in cat_links]
        results = await asyncio.gather(*tasks)

        url_list = []

        url_list = url_list[0:3]

        for links in results:
            url_list.extend(links)

        logger.info(f"Bắt đầu trích xuất chi tiết {len(url_list)} sản phẩm...")
        
        await process_request_product(context, url_list, 5)

        await context.close()

if __name__ == "__main__":
    asyncio.run(crawler())