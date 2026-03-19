import asyncio
import json
import re
import logging
import os
from datetime import datetime, timezone
from math import pow

from supabase import create_client, Client
from dotenv import load_dotenv
import redis

# ==========================================
# CẤU HÌNH LOGGING & DATABASE SUPABASE
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataConsumer")

# Tắt các dòng log INFO (HTTP Request, v.v.) nhưng vẫn giữ lại WARNING và ERROR
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("ultralytics").setLevel(logging.WARNING)

load_dotenv()

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if SUPABASE_URL and SUPABASE_KEY:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("✓ Kết nối Supabase thành công")
else:
    logger.warning("✗ Thiếu biến môi trường Supabase! Các thao tác DB sẽ bị bỏ qua.")
    supabase = None

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "tiktok_shop_raw_data")

try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=False
    )
    redis_client.ping()
    logger.info(f"✓ Kết nối Redis thành công: {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    logger.error(f"✗ Không thể kết nối Redis: {e}")
    redis_client = None

# ==========================================
# CÁC HÀM XỬ LÝ DỮ LIỆU (PARSING & EXTRACTION)
# ==========================================
def extract_json_from_html_sync(text_data, url):
    """
    Bóc tách JSON từ HTML bằng Regex.
    Chạy đồng bộ, được gọi từ asyncio.to_thread() để không block.
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
    """Parse mô tả sản phẩm từ định dạng TikTok"""
    if not raw_desc:
        return ""
    try:
        desc_blocks = json.loads(raw_desc)
        texts = [block.get("text", "").strip() for block in desc_blocks if block.get("type") == "text" and block.get("text", "").strip()]
        return " ".join(texts)
    except json.JSONDecodeError:
        return str(raw_desc).strip()

async def parse_number(text):
    """Parse số từ chuỗi (hỗ trợ K, M)"""
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

async def calculate_weight_time(review_date: datetime):
    """Tính trọng lượng thời gian cho review"""
    now = datetime.now(timezone.utc)
    delta_t = (now - review_date).days
    return pow(0.5, delta_t / 30.0)

# ==========================================
# DATABASE UPSERT LOGIC
# ==========================================
async def upsert_to_supabase(structured_data):
    """
    Đẩy dữ liệu đã làm sạch lên Database Supabase.
    Sử dụng Upsert để tránh duplicate data khi tracking sự thay đổi của thị trường.
    """
    if not supabase:
        logger.warning("Supabase client không khả dụng, bỏ qua upsert")
        return
        
    try:
        # Upsert Shops
        if structured_data.get("shops"):
            supabase.table("shops").upsert(structured_data["shops"]).execute()
            logger.info(f"✓ Upserted {len(structured_data['shops'])} shops")
            
        # Upsert Products
        if structured_data.get("products"):
            supabase.table("products").upsert(structured_data["products"]).execute()
            logger.info(f"✓ Upserted {len(structured_data['products'])} products")

        # Insert/Upsert Product Metrics History
        if structured_data.get("products_metrics_history"):
            supabase.table("products_metrics_history").insert(structured_data["products_metrics_history"]).execute()
            logger.info(f"✓ Inserted {len(structured_data['products_metrics_history'])} product metrics")
            
        # Upsert Reviews (chia lô nếu quá lớn)
        if structured_data.get("reviews"):
            reviews = structured_data["reviews"]
            # Chia lô 100 bản ghi để tránh quá tải
            batch_size = 100
            for i in range(0, len(reviews), batch_size):
                batch = reviews[i:i+batch_size]
                supabase.table("reviews").upsert(batch).execute()
            logger.info(f"✓ Upserted {len(reviews)} reviews")

    except Exception as e:
        logger.error(f"✗ Lỗi khi đẩy dữ liệu lên Supabase: {e}")

# ==========================================
# LOGIC TRÍCH XUẤT DỮ LIỆU CẤU TRÚC
# ==========================================
async def extract_tiktok_data(json_data_list):
    """
    Trích xuất và cấu trúc hóa dữ liệu từ danh sách JSON thô.
    Bóc tách shops, products, reviews từ payload.
    """
    shops = {}
    products = {}
    products_metrics_history = {}
    reviews = {}

    for item in json_data_list:
        url = item.get("url", "")
        payload = item.get("data", {})

        if "pdp" in url:
            logger.debug(f"Đang xử lý PDP: {url}")

        # 1. TRÍCH XUẤT TỪ PDP (Product Detail Page)
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
                    "scraped_at": datetime.now(timezone.utc).isoformat()
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
                        "review_time": dt_obj.isoformat() if dt_obj else None
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
                        "review_time": dt_obj.isoformat() if dt_obj else None
                    }
    
    return {
        "shops": list(shops.values()),
        "products": list(products.values()),
        "products_metrics_history": list(products_metrics_history.values()),
        "reviews": list(reviews.values())
    }

# ==========================================
# CONSUMER WORKER - LẮNG NGHE REDIS & XỬ LÝ
# ==========================================
async def consume_raw_data():
    """
    Worker chính: Liên tục lắng nghe Redis, bóc tách dữ liệu, và đẩy lên Supabase.
    """
    if not redis_client:
        logger.error("✗ Redis client không khả dụng! Không thể khởi động consumer.")
        return
    
    logger.info(f"🚀 Data Consumer đã khởi động, lắng nghe queue: {QUEUE_NAME}")
    
    batch_data = []
    batch_size = 10  # Ghép 10 items trước khi xử lý
    processed_count = 0
    
    while True:
        try:
            # Lấy dữ liệu từ Redis (timeout 1s)
            result = redis_client.brpop(QUEUE_NAME, timeout=1)
            
            if result:
                # result = (queue_name, data)
                queue_name, raw_json = result
                
                try:
                    data_item = json.loads(raw_json)
                    batch_data.append(data_item)
                    logger.debug(f"📥 Nhận dữ liệu từ: {data_item.get('url', 'unknown')}")
                    
                    # Nếu batch đủ, xử lý luôn
                    if len(batch_data) >= batch_size:
                        logger.info(f"⏱️  Xử lý batch {len(batch_data)} items...")
                        
                        # Bóc tách HTML nếu có
                        html_items = [item for item in batch_data if item.get("type") == "html"]
                        json_items = [item for item in batch_data if item.get("type") != "html"]
                        
                        for html_item in html_items:
                            try:
                                chunks = await asyncio.to_thread(
                                    extract_json_from_html_sync,
                                    html_item.get("data", ""),
                                    html_item.get("url", "")
                                )
                                json_items.extend(chunks)
                            except Exception as e:
                                logger.error(f"Lỗi parse HTML: {e}")
                        
                        # Trích xuất dữ liệu cấu trúc
                        if json_items:
                            structured_data = await extract_tiktok_data(json_items)
                            await upsert_to_supabase(structured_data)
                            processed_count += len(json_items)
                        
                        batch_data = []
                
                except json.JSONDecodeError as e:
                    logger.error(f"✗ Lỗi parse JSON từ Redis: {e}")
                    
            else:
                # Timeout - không có data mới
                # Xử lý dữ liệu tồn đọng nếu có
                if batch_data:
                    logger.info(f"⏱️  Xử lý batch còn lại {len(batch_data)} items...")
                    
                    html_items = [item for item in batch_data if item.get("type") == "html"]
                    json_items = [item for item in batch_data if item.get("type") != "html"]
                    
                    for html_item in html_items:
                        try:
                            chunks = await asyncio.to_thread(
                                extract_json_from_html_sync,
                                html_item.get("data", ""),
                                html_item.get("url", "")
                            )
                            json_items.extend(chunks)
                        except Exception as e:
                            logger.error(f"Lỗi parse HTML: {e}")
                    
                    if json_items:
                        structured_data = await extract_tiktok_data(json_items)
                        await upsert_to_supabase(structured_data)
                        processed_count += len(json_items)
                    
                    batch_data = []
                
        except KeyboardInterrupt:
            logger.info("⛔ Consumer dừng lại bởi người dùng (Ctrl+C)")
            break
        except Exception as e:
            logger.error(f"✗ Lỗi consumer: {e}")
            await asyncio.sleep(2)  # Tunggu trước khi retry
    
    logger.info(f"✓ Consumer kết thúc. Đã xử lý {processed_count} items.")

async def main():
    """Entry point cho Data Consumer"""
    await consume_raw_data()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Tạm dừng Data Consumer")
