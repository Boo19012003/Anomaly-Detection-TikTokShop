import random
import asyncio
import os
import sys
import logging
from datetime import datetime
from playwright.async_api import async_playwright
from supabase import create_client, Client
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("playwright").setLevel(logging.WARNING)

file_handler = RotatingFileHandler(
    "reviews_async.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding='utf-8'
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[file_handler, logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

sys.path.append(os.path.abspath("../"))
from captcha_solver_async import solve_tiktok_captcha

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Các hàm thao tác Database (chạy trong Thread riêng để không block Event Loop) ---
async def fetch_pending_products():
    def _fetch():
        return supabase.table("products") \
            .select("product_id, link") \
            .eq("is_reviewed", False) \
            .order("product_id", desc=True) \
            .limit(50) \
            .execute()
    res = await asyncio.to_thread(_fetch)
    return res.data

async def delete_product_db(product_id):
    def _delete():
        return supabase.table("products").delete().eq("product_id", product_id).execute()
    await asyncio.to_thread(_delete)

async def update_product_reviewed_db(product_id):
    def _update():
        return supabase.table("products").update({"is_reviewed": True}).eq("product_id", product_id).execute()
    await asyncio.to_thread(_update)

async def upsert_reviews_db(unique_reviews):
    def _upsert():
        return supabase.table("reviews").upsert(unique_reviews).execute()
    await asyncio.to_thread(_upsert)


async def solve_captcha(page):
    captcha_count = 0
    max_retries = 5

    while captcha_count < max_retries:
        status = await solve_tiktok_captcha(page)

        if status == "no_captcha":
            break
        elif status == "success":
            logger.info(f"[Captcha Solver] Captcha giải thành công trên tab {page.url}.")
            await asyncio.sleep(2)
            break
        else:
            logger.warning(f"[Solve Captcha] Giải captcha thất bại, thử lại lần {captcha_count + 1}/{max_retries}")
            captcha_count += 1
            await asyncio.sleep(3)

            if captcha_count == max_retries:
                logger.warning("[Solve Captcha] Cảnh báo: Không thể giải captcha sau nhiều lần thử. Bỏ qua.")


# Hàm xử lý từng sản phẩm (Mỗi lần gọi là 1 tab mới)
async def process_product(context, product, semaphore):
    product_id = product["product_id"]
    url = product["link"]

    async with semaphore:
        page = await context.new_page()
        
        # Mảng dữ liệu được gom gọn cục bộ cho riêng từng tab
        pending_reviews = []

        async def handle_response(response):
            if "api/shop/pdp_desktop/get_product_reviews" in response.url:
                try:
                    data = await response.json()
                    reviews_data = data.get("data", {}).get("product_reviews", [])

                    if not reviews_data:
                        return

                    for review in reviews_data:
                        ts = int(review.get("review_time"))
                        if ts > 9999999999:
                            ts = ts / 1000
                        formatted_time = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

                        pending_reviews.append({
                            "review_id": str(review.get("review_id")),
                            "product_id": str(review.get("product_id")),
                            "sku_id": str(review.get("sku_id")),
                            "review_rating": review.get("review_rating"),
                            "review_time": formatted_time,
                            "review_text": review.get("review_text"),
                            "sku_specification": review.get("sku_specification"),
                        })
                except Exception as e:
                    logger.warning(f"[{product_id}] Lỗi xử lý response: {e}")

        page.on("response", handle_response)

        try:
            logger.info(f"[Scrap Reviews] Mở tab xử lý sản phẩm {product_id}")
            await page.goto(url, timeout=30000)

            await solve_captcha(page)

            unavailable_locator = page.locator("text='Product not available in this country or region'")
            try:
                await unavailable_locator.wait_for(timeout=3000)
                logger.warning(f"[Scrap Reviews] Sản phẩm {product_id} không tồn tại/bị chặn. Đang xóa...")
                await delete_product_db(product_id)
                return
            except:
                pass 
            
            try:
                async with page.expect_response("**/api/shop/pdp_desktop/get_product_reviews**", timeout=10000):
                    await page.locator("div[data-testid='tux-web-interaction-container']:has-text('Verified purchase')").click(force=True)
            except Exception as e:
                logger.warning(f"[{product_id}] Lỗi khi click 'Verified purchase': {e}")

            pagination_locator = page.locator("div.flex.gap-6 > div").last
            try:
                await pagination_locator.wait_for(state="visible", timeout=3000)
                total_pages_str = await pagination_locator.inner_text()
                total_pages = int(total_pages_str)
            except:
                total_pages = 1

            logger.info(f"[{product_id}] Tổng số trang đánh giá: {total_pages}")

            if total_pages > 1:
                for i in range(2, total_pages + 1):
                    try:
                        async with page.expect_response("**/api/shop/pdp_desktop/get_product_reviews**", timeout=20000):
                            await page.locator("div.Headline-Semibold:has-text('Next')").click()
                    except Exception as e:
                        logger.warning(f"[{product_id}] Lỗi khi sang trang {i}: {e}")
                        break
            
            # Đợi thêm một chút để các sự kiện mạng nền kịp phân tích json
            await asyncio.sleep(1)

            actual_reviews_count = len(pending_reviews)
            logger.info(f"[{product_id}] Đã thu thập được {actual_reviews_count} đánh giá")

            if actual_reviews_count > 0:
                unique_reviews_dict = {r["review_id"]: r for r in pending_reviews}
                unique_reviews = list(unique_reviews_dict.values())
                logger.info(f"[{product_id}] Đang lưu {len(unique_reviews)} đánh giá vào Database.")
                await upsert_reviews_db(unique_reviews)
            
            await update_product_reviewed_db(product_id)
        
        except Exception as e:
            logger.warning(f"[{product_id}] Lỗi khi xử lý toàn cục: {e}")
        finally:
            await page.close()


async def main():
    # Giới hạn số lượng Tab chạy đồng thời. Bạn có thể tăng/giảm con số này tuỳ sức mạnh máy tính.
    MAX_CONCURRENT_TABS = 5
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)

    async with async_playwright() as p:
        args = [
            '--disable-blink-features=AutomationControlled',
            '--start-maximized',
            '--disable-infobars',
            '--no-sandbox'
        ]

        context = await p.chromium.launch_persistent_context(
            user_data_dir="reviews",
            headless=False,
            channel="chrome",
            args=args,
            viewport=None,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        while True:
            items = await fetch_pending_products()
            
            if not items:
                logger.info("[Scrap Reviews] Không có sản phẩm nào cần thu thập đánh giá. Kết thúc.")
                break

            tasks = []
            for item in items:
                task = asyncio.create_task(process_product(context, item, semaphore))
                tasks.append(task)
            
            # Chờ tất cả 50 sản phẩm trong batch hiện tại chạy xong (với tối đa MAX_CONCURRENT_TABS tại 1 thời điểm)
            await asyncio.gather(*tasks)

        await context.close()

if __name__ == "__main__":
    asyncio.run(main())