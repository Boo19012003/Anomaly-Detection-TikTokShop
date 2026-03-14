"""
update_metrics.py
-----------------
Script cập nhật bảng products_metrics_history mỗi 4 giờ.
Quy trình:
  1. Đọc danh sách sản phẩm (product_id + product_url) từ Supabase
  2. Crawl từng trang sản phẩm bằng Playwright
  3. Insert bản ghi mới vào products_metrics_history
"""
import asyncio
import os
import sys
import json
import re
import logging
import random
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from playwright.async_api import async_playwright
from supabase import create_client, Client
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from captcha_solver import solve_tiktok_captcha

# ==========================================
# LOGGING & DATABASE
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MetricsUpdater")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("ultralytics").setLevel(logging.WARNING)
logging.getLogger("playwright").setLevel(logging.WARNING)

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Thiếu biến môi trường SUPABASE_URL hoặc SUPABASE_KEY!")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# TIỆN ÍCH
# ==========================================
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
        multiplier = 1_000_000
        text = text[:-1]
    try:
        value = float(text) * multiplier
        return int(value) if value.is_integer() else value
    except ValueError:
        return 0

def extract_json_from_html_sync(text_data, url):
    """Hàm đồng bộ chạy trên ThreadPool để không block Event Loop."""
    result = []
    json_matches = re.finditer(
        r'<script[^>]*type=["\']application/json["\'][^>]*>([\s\S]*?)</script>', text_data
    )
    for match in json_matches:
        try:
            content = match.group(1).strip()
            if content:
                result.append({"url": url, "type": "text_json", "data": json.loads(content)})
        except json.JSONDecodeError:
            pass

    unv = re.search(
        r'<script[^>]*id=["\']__UNIVERSAL_DATA_FOR_REHYDRATION__["\'][^>]*>([\s\S]*?)</script>',
        text_data
    )
    if unv:
        try:
            result.append({"url": url, "type": "universal_data", "data": json.loads(unv.group(1).strip())})
        except json.JSONDecodeError:
            pass
    return result

async def check_captcha_visible(page):
    selectors = "#captcha_container, .captcha_verify_container, #tts_web_captcha_container"
    locators = page.locator(selectors)
    for i in range(await locators.count()):
        if await locators.nth(i).is_visible():
            return True
    return False

async def solve_captcha_async(page):
    for attempt in range(5):
        try:
            status = await solve_tiktok_captcha(page)
        except Exception as e:
            logger.warning(f"Lỗi captcha module: {e}")
            break
        if status in ("no_captcha", "success"):
            await asyncio.sleep(0.5)
            break
        logger.warning(f"[Captcha] Thử lại {attempt + 1}/5")
        await asyncio.sleep(1)

async def intercept_route(route):
    url = route.request.url.lower()
    if "ibyteimg.com" in url and ("-origin-jpeg.jpeg" in url or "-origin-png.png" in url):
        await route.continue_()
        return
    if route.request.resource_type in ["image", "media", "font"]:
        await route.abort()
    else:
        await route.continue_()

# ==========================================
# TRÍCH XUẤT METRICS TỪ PDP
# ==========================================
async def extract_metrics_from_json(json_data_list):
    """Chỉ trích xuất metrics (giá, tồn kho, đánh giá...) từ data đã crawl."""
    metrics_list = []

    for item in json_data_list:
        url  = item.get("url", "")
        payload = item.get("data", {})

        if "pdp" not in url or not isinstance(payload, dict):
            continue
        if "loaderData" not in payload:
            continue

        loader_data = payload.get("loaderData", {})
        pdp_page    = loader_data.get(
            "shop/(region)/pdp/(product_name_slug$)/(product_id)/page", {}
        )
        if not pdp_page:
            continue

        page_config    = pdp_page.get("page_config", {})
        components_map = page_config.get("components_map", [])

        product_info_block = {}
        component_data     = {}
        for comp in components_map:
            if comp.get("component_type") == "product_info":
                component_data     = comp.get("component_data", {})
                product_info_block = component_data.get("product_info", {})
                break

        if not product_info_block:
            continue

        product_detail = product_info_block.get("product_model", {}).get("product_detail", {})
        product_id     = product_detail.get("product_id")
        if not product_id:
            continue

        skus           = product_detail.get("skus", [])
        total_quantity = sum(
            sku.get("sku_quantity", {}).get("available_quantity", 0) for sku in skus
        )

        promotion_tag = component_data.get("promotion_tag", {}).get("placement_labels", {})
        deal_text     = (
            promotion_tag.get("1", [{}])[0].get("text") if promotion_tag.get("1") else None
        )

        metrics_list.append({
            "product_id":       product_id,
            "sold_count":       await parse_number(product_detail.get("sold", "0")),
            "review_count":     await parse_number(
                product_info_block.get("review_model", {}).get("product_review_count", "0")
            ),
            "rating_avg":       await parse_number(
                product_info_block.get("review_model", {}).get("product_overall_score", 0.0)
            ),
            "quantity":         total_quantity,
            "price":            await parse_number(
                product_info_block.get("promotion_model", {})
                .get("price_view", {})
                .get("min_price_item", {})
                .get("real_price")
            ),
            "discount_percent": await parse_number(
                product_info_block.get("promotion_model", {})
                .get("price_view", {})
                .get("min_price_item", {})
                .get("discount")
            ),
            "is_flash_sale":    deal_text == "Flash sale",
            "is_deal":          deal_text == "Deal",
            "is_anomaly":       False,
            "anomaly_score":    0.0,
            "scraped_at":       datetime.now(timezone.utc).isoformat(),
        })

    return metrics_list

# ==========================================
# CRAWL MỘT URL & LẤY METRICS
# ==========================================
async def crawl_product_metrics(context, url, semaphore):
    async with semaphore:
        page        = await context.new_page()
        raw_data    = []

        async def handle_response(response):
            if "/shop/vn/" in response.url and response.status == 200:
                try:
                    ct = response.headers.get("content-type", "").lower()
                    if "application/json" in ct:
                        raw_data.append({"url": response.url, "type": "json", "data": await response.json()})
                    elif "text" in ct:
                        chunks = await asyncio.to_thread(
                            extract_json_from_html_sync, await response.text(), response.url
                        )
                        raw_data.extend(chunks)
                except Exception as e:
                    logger.error(f"Lỗi parse response: {e}")

        page.on("response", handle_response)

        try:
            await page.goto(url, timeout=20000)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass

            if await check_captcha_visible(page):
                await solve_captcha_async(page)
        except Exception as e:
            logger.error(f"[Metrics] Lỗi khi load {url}: {e}")
        finally:
            await page.close()

        if raw_data:
            return await extract_metrics_from_json(raw_data)
        return []

# ==========================================
# INSERT METRICS VÀO SUPABASE
# ==========================================
async def insert_metrics_batch(metrics_list):
    if not metrics_list:
        return
    try:
        supabase.table("products_metrics_history").insert(metrics_list).execute()
        logger.info(f"✅ Inserted {len(metrics_list)} bản ghi metrics vào Supabase")
    except Exception as e:
        logger.error(f"Lỗi Supabase insert: {e}")

# ==========================================
# MAIN
# ==========================================
async def update_metrics():
    # 1. Lấy danh sách sản phẩm từ Supabase (có product_url)
    logger.info("Đang lấy danh sách sản phẩm từ Supabase...")
    try:
        response = supabase.table("products").select("product_id, product_url").execute()
        products = response.data or []
    except Exception as e:
        logger.error(f"Không thể lấy dữ liệu từ Supabase: {e}")
        return

    # Lọc các sản phẩm có URL hợp lệ
    url_list = [p["product_url"] for p in products if p.get("product_url")]
    logger.info(f"Tìm thấy {len(url_list)} sản phẩm cần cập nhật metrics.")

    if not url_list:
        logger.warning("Không có URL nào để crawl. Kiểm tra lại cột product_url trong Supabase.")
        return

    # 2. Khởi động Playwright
    async with async_playwright() as p:
        args = [
            '--disable-blink-features=AutomationControlled',
            '--window-size=1280,720',
            '--no-sandbox',
            '--disable-infobars',
        ]
        headless = os.getenv("HEADLESS", "true").lower() != "false"

        logger.info(f"Mở trình duyệt (headless={headless})...")
        browser = await p.chromium.launch(headless=headless, channel="chrome", args=args)

        state_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "auth", "state.json")
        )
        context_args = {
            "viewport": {"width": 1280, "height": 720},
            "user_agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ])
        }
        if os.path.exists(state_path):
            context_args["storage_state"] = state_path
        else:
            logger.warning(f"Không tìm thấy auth session tại: {state_path}")

        context = await browser.new_context(**context_args)
        await context.route("**/*", intercept_route)

        # 3. Crawl song song (tối đa 3 tab cùng lúc)
        MAX_TABS    = 3
        semaphore   = asyncio.Semaphore(MAX_TABS)
        all_metrics = []

        tasks   = [crawl_product_metrics(context, url, semaphore) for url in url_list]
        results = await asyncio.gather(*tasks)

        for metrics in results:
            all_metrics.extend(metrics)

        await context.close()

    # 4. Insert tất cả metrics vào Supabase
    logger.info(f"Tổng số metrics thu thập được: {len(all_metrics)}")
    await insert_metrics_batch(all_metrics)
    logger.info("✅ Hoàn thành cập nhật metrics!")


if __name__ == "__main__":
    asyncio.run(update_metrics())
