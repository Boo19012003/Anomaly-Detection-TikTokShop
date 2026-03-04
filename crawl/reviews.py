import random

from playwright.sync_api import sync_playwright
import time
import os
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
import sys
import logging
from logging.handlers import RotatingFileHandler
from captcha_solver import solve_tiktok_captcha

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("playwright").setLevel(logging.WARNING)

file_handler = RotatingFileHandler(
    "reviews.log",
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=3,
    encoding='utf-8'
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        file_handler,
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def solve_captcha(page):
    captcha_count = 0
    max_retries = 5

    while captcha_count < max_retries:
        status = solve_tiktok_captcha(page)

        if status == "no_captcha":
            break
        elif status == "success":
            logger.info("[Captcha Solver] Captcha đã được giải thành công.")
            time.sleep(2)
            break

        else:
            logger.warning(f"[Solve Captcha] Giải captcha thất bại, thử lại lần {captcha_count + 1}/{max_retries}")
            captcha_count += 1
            time.sleep(3)

            if captcha_count == max_retries:
                logger.warning("[Solve Captcha] Cảnh báo: Không thể giải captcha sau nhiều lần thử. Vui lòng kiểm tra thủ công.")

def reviews():
    with sync_playwright() as p:
        args = [
            '--disable-blink-features=AutomationControlled',
            '--start-maximized',
            '--disable-infobars',
            '--no-sandbox'
        ]

        context = p.chromium.launch_persistent_context(
            user_data_dir="reviews",
            headless=False,
            channel="chrome",
            args=args,
            viewport=None,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        page = context.pages[0]


        pending_reviews = []

        def handle_response(response):
            if "api/shop/pdp_desktop/get_product_reviews" in response.url:
                try:
                    data = response.json()
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
                    logger.warning(f"[Scrap Reviews] Lỗi xử lý: {e}")
        
        
        page.on("response", handle_response)

        while True:
            # res = supabase.table("products") \
            #     .select("product_id, link") \
            #     .eq("is_reviewed", False) \
            #     .order("product_id", desc=True) \
            #     .limit(50) \
            #     .execute()

            # Đoạn code này dùng để test với 1 sản phẩm cụ thể
            res = supabase.table("products") \
                .select("product_id, link") \
                .eq("product_id", "1733272891643167965") \
                .execute()

            items = res.data
            
            if not items:
                logger.info("[Scrap Reviews] Không có sản phẩm nào cần thu thập đánh giá")
                break

            for item in items:
                product_id = item["product_id"]
                url = item["link"]

                pending_reviews.clear()

                try:
                    logger.info(f"[Scrap Reviews] Đang thu thập đánh giá cho sản phẩm {product_id}")
                    page.goto(url, timeout=30000)

                    # Giải Captcha nếu có
                    solve_captcha(page)

                    # Check sản phẩm còn tồn tại hay không
                    unavailable_locator = page.locator("text='Product not available in this country or region'")
                    try:
                        unavailable_locator.wait_for(timeout=3000)
                        logger.warning(f"[Scrap Reviews] Sản phẩm {product_id} không tồn tại/bị chặn. Đang xóa...")
                        supabase.table("products").delete().eq("product_id", product_id).execute()
                        continue
                    except:
                        pass # Sản phẩm bình thường, tiếp tục
                    
                    try:
                        with page.expect_response("**/api/shop/pdp_desktop/get_product_reviews**", timeout=10000):
                            page.locator("div[data-testid='tux-web-interaction-container']:has-text('Verified purchase')").click(force=True)
                    except Exception as e:
                        logger.warning(f"[Scrap Reviews] Lỗi khi click 'Verified purchase': {e}")

                    pagination_locator = page.locator("div.flex.gap-6 > div").last
                    try:
                        pagination_locator.wait_for(state="visible", timeout=3000)
                        total_pages_str = pagination_locator.inner_text()
                        total_pages = int(total_pages_str)
                    except:
                        total_pages = 1

                    logger.info(f"[Scrap Reviews] Tổng số trang đánh giá: {total_pages}")

                    if total_pages > 1:
                        for i in range(2, total_pages + 1):
                            try:
                                with page.expect_response("**/api/shop/pdp_desktop/get_product_reviews**", timeout=20000):
                                    page.locator("div.Headline-Semibold:has-text('Next')").click()
                            except Exception as e:
                                logger.warning(f"[Scrap Reviews] Lỗi khi sang trang {i}: {e}")
                                break
                    
                    actual_reviews_count = len(pending_reviews)
                    logger.info (f"[Scrap Reviews] Đã thu thập được {actual_reviews_count} đánh giá")
 
                    if actual_reviews_count > 0:
                        unique_reviews_dict = {reviews["review_id"]: reviews for reviews in pending_reviews}
                        unique_reviews = list(unique_reviews_dict.values())

                        logger.info(f"[Scrap Reviews] Đang lưu {len(unique_reviews)} đánh giá vào Database.")

                        supabase.table("reviews").upsert(unique_reviews).execute()

                        pending_reviews.clear()
                    
                    supabase.table("products").update({"is_reviewed": True}).eq("product_id", product_id).execute()
                
                except Exception as e:
                    logger.warning(f"[Scrap Reviews] Lỗi khi xử lý sản phẩm {product_id}: {e}")


        context.close()
    
if __name__ == "__main__":
    reviews()