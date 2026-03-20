import json
import re
from datetime import datetime, timezone
from app.config.settings import get_logger
from app.parser.nlp_utils import parse_tiktok_description

logger = get_logger("ReviewParser")

async def extract_review(json_data_list):
    reviews = {}

    for item in json_data_list:
        url = item.get("url", "")
        payload = item.get("data", {})

        if "get_product_reviews" in url and isinstance(payload, dict):
            api_data = payload.get("data", {})
            paginated_reviews = api_data.get("product_reviews", [])
            
            if not paginated_reviews:
                continue
                
            for rev in paginated_reviews:
                rev_id = rev.get("review_id")
                if rev_id and rev_id not in reviews:
                    raw_text = rev.get("review_text", "")
                    if len(raw_text.split()) > 10:
                        ts_ms = int(rev.get("review_time", 0))
                        dt_obj = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date() if ts_ms else None
                        
                        parsed_text = await parse_tiktok_description(raw_text)
                        
                        reviews[rev_id] = {
                            "review_id": rev_id,
                            "product_id": rev.get("product_id"),
                            "sku_id": rev.get("sku_id"),
                            "sku_specification": rev.get("sku_specification"),
                            "rating": rev.get("review_rating"), 
                            "text": await parse_tiktok_description(parsed_text),
                            "is_verified": rev.get("is_verified_purchase", False),
                            "review_time": dt_obj.isoformat() if dt_obj else None
                        }
    
    return {
        "reviews": list(reviews.values())
    }
