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
    
    return {
        "shops": list(shops.values()),
        "products": list(products.values()),
    }
