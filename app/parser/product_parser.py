import json
import re
from datetime import datetime, timezone
from app.config.settings import get_logger
from app.parser.nlp_utils import parse_number, parse_tiktok_description

logger = get_logger("ProductParser")

async def extract_product(json_data_list):
    shops = {}
    products = {}
    products_metrics_history = {}

    for item in json_data_list:
        url = item.get("url", "")
        payload = item.get("data", {})

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
            
            if not product_info_block or not component_data:
                continue

            product_model = product_info_block.get("product_model", {})
            product_id = product_model.get("product_id")
            
            if not product_id:
                continue

            # --- THÔNG TIN SHOP ---
            shop_info = component_data.get("shop_info", {}) 
            seller_model = product_info_block.get("seller_model", {})
            shop_id = shop_info.get("seller_id", product_model.get("seller_id"))
            
            if shop_id and shop_id not in shops:
                store_sub_score = shop_info.get("store_sub_score", seller_model.get("store_sub_score", []))
                response_rate_score = next((score.get("score_percentage") for score in store_sub_score if score.get("type") == 1), "0")

                shops[shop_id] = {
                    "shop_id": shop_id,
                    "name": shop_info.get("shop_name", seller_model.get("shop_name")),
                    "rating_avg": await parse_number(shop_info.get("shop_rating", seller_model.get("shop_rating", "0"))),
                    "response_rate": await parse_number(response_rate_score),
                    "followers_count": await parse_number(shop_info.get("followers_count", "0")),
                    "is_rising_star": False,
                    "safe_score": 0.0,
                }

            # --- THÔNG TIN SẢN PHẨM ---
            if product_id and product_id not in products:
                categories = component_data.get("categories", [])
                category_name = next((c.get("category_name") for c in categories if c.get("level") == 1), categories[0].get("category_name") if categories else None)

                products[product_id] = {
                    "product_id": product_id,
                    "shop_id": shop_id,
                    "category": category_name,
                    "title": product_model.get("name"),
                    "description": await parse_tiktok_description(product_model.get("description", "")),
                    "product_url": url,
                }

            # --- CẬP NHẬT METRICS & LỊCH SỬ ---
            if product_id and product_id not in products_metrics_history:
                skus = product_info_block.get("skus", product_model.get("skus", [])) 
                total_quantity = sum(sku.get("sku_quantity", {}).get("available_quantity", 0) for sku in skus)
                
                promotion_tag = component_data.get("promotion_tag", {}).get("placement_labels", {})
                deal_text = None
                for key, labels in promotion_tag.items():
                    for label in labels:
                        if label.get("text") in ["Deal", "Flash sale"]:
                            deal_text = label.get("text")
                            break
                    if deal_text:
                        break

                # Bóc tách Giá gốc (origin_price) & Giá sale
                promotion_model = product_info_block.get("promotion_model", {})
                min_price_data = promotion_model.get("promotion_product_price", {}).get("min_price", {})
                
                origin_price = min_price_data.get("origin_price_decimal", "0")
                sale_price = min_price_data.get("sale_price_decimal", "0")
                discount_decimal = min_price_data.get("discount_decimal", "0")

                # Bóc tách dữ liệu Review từng sao
                review_info = component_data.get("review_info", {})
                rating_result = review_info.get("review_ratings", {}).get("rating_result", {})

                products_metrics_history[product_id] = {
                    "product_id": product_id,
                    "origin_price": await parse_number(origin_price),
                    "sale_price": await parse_number(sale_price),
                    "discount_percent": int(float(discount_decimal) * 100) if discount_decimal else 0,
                    "quantity": total_quantity,
                    "sold_count": await parse_number(product_model.get("sold_count", "0")),
                    "rating_avg": await parse_number(product_info_block.get("review_model", {}).get("product_overall_score", 0.0)),
                    "review_count": await parse_number(product_info_block.get("review_model", {}).get("product_review_count", "0")),
                    "count_5_star": await parse_number(rating_result.get("5", "0")),
                    "count_4_star": await parse_number(rating_result.get("4", "0")),
                    "count_3_star": await parse_number(rating_result.get("3", "0")),
                    "count_2_star": await parse_number(rating_result.get("2", "0")),
                    "count_1_star": await parse_number(rating_result.get("1", "0")),
                    "is_flash_sale": deal_text == "Flash sale",
                    "is_deal": deal_text == "Deal",
                    "is_anomaly": False,
                    "anomaly_score": 0.0,
                    "scraped_at": datetime.now(timezone.utc).isoformat()
                }
    
    return {
        "shops": list(shops.values()),
        "products": list(products.values()),
        "products_metrics_history": list(products_metrics_history.values()),
    }
