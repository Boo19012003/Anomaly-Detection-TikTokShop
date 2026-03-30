from app.database.connection import get_supabase_client
from app.config.settings import get_logger
from postgrest.exceptions import APIError
import httpx

logger = get_logger("CRUD")
supabase = get_supabase_client()

async def upsert_to_supabase(structured_data):
    if not supabase:
        logger.warning("Supabase client not available, skipping upsert")
        return

    if not isinstance(structured_data, dict):
        logger.error(f"Invalid structured_data format: Expected dict, got {type(structured_data)}")
        return
        
    try:
        inserted_any = False
        if structured_data.get("shops"):
            supabase.table("shops").upsert(structured_data["shops"]).execute()
            inserted_any = True
            
        if structured_data.get("products"):
            supabase.table("products").upsert(structured_data["products"]).execute()
            inserted_any = True

        if structured_data.get("products_metrics_history"):
            supabase.table("products_metrics_history").insert(structured_data["products_metrics_history"]).execute()
            inserted_any = True
            
        if structured_data.get("reviews"):
            reviews = structured_data["reviews"]
            if reviews:
                batch_size = 100
                for i in range(0, len(reviews), batch_size):
                    batch = reviews[i:i+batch_size]
                    supabase.table("reviews").upsert(batch).execute()
                inserted_any = True

        if not inserted_any:
            logger.warning("No data found to upsert to Supabase")
        
    except APIError as e:
        logger.error(f"Supabase Database Error [APIError]: {e.message} - Code: {e.code} - Details: {e.details}")
    except httpx.TimeoutException as e:
        logger.error(f"Network Timeout Error interacting with Supabase: {e}")
    except httpx.RequestError as e:
        logger.error(f"Network Connection Error interacting with Supabase: {e}")
    except AttributeError as e:
        logger.error(f"Data Structure Error (AttributeError): {e}")
    except Exception as e:
        logger.error(f"Unexpected Error during Supabase upsert: {e}")

async def get_product_links_from_supabase():
    if not supabase:
        logger.warning("Supabase client not available, skipping upsert")
        return []
    try:
        response = supabase.table("products").select("product_url").execute()
        return [row["product_url"] for row in response.data]

    except APIError as e:
        logger.error(f"Supabase Database Error [APIError]: {e.message}")
        return []
    except httpx.TimeoutException as e:
        logger.error(f"Network Timeout Error interacting with Supabase: {e}")
        return []
    except httpx.RequestError as e:
        logger.error(f"Network Connection Error interacting with Supabase: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error in get_product_links_from_supabase: {e}")
        return []

async def get_uncrawled_product_links_from_supabase(limit=10):
    if not supabase:
        logger.warning("Supabase client not available, skipping fetch")
        return []
    try:
        response = supabase.table("products").select("product_url").or_("is_review_crawled.is.null,is_review_crawled.eq.false").limit(limit).execute()
        return [row["product_url"] for row in response.data]

    except APIError as e:
        logger.error(f"Supabase Database Error fetch uncrawled links: {e.message}")
        return []
    except httpx.TimeoutException as e:
        logger.error(f"Network Timeout Error fetch uncrawled links: {e}")
        return []
    except httpx.RequestError as e:
        logger.error(f"Network Connection Error fetch uncrawled links: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error in get_uncrawled_product_links_from_supabase: {e}")
        return []

async def mark_product_as_crawled(url: str):
    if not supabase:
        return
    try:
        supabase.table("products").update({"is_review_crawled": True}).eq("product_url", url).execute()

    except APIError as e:
        logger.error(f"Supabase update status error [APIError]: {e.message}")
    except httpx.TimeoutException as e:
        logger.error(f"Network Timeout Error update status: {e}")
    except httpx.RequestError as e:
        logger.error(f"Network Connection Error update status: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in mark_product_as_crawled: {e}")