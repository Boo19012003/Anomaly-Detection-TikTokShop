from app.database.connection import get_supabase_client
from app.config.settings import get_logger

logger = get_logger("CRUD")
supabase = get_supabase_client()

async def upsert_to_supabase(structured_data):
    if not supabase:
        logger.warning("Supabase client not available, skipping upsert")
        return
        
    try:
        if structured_data.get("shops"):
            supabase.table("shops").upsert(structured_data["shops"]).execute()
            
        if structured_data.get("products"):
            supabase.table("products").upsert(structured_data["products"]).execute()

        if structured_data.get("products_metrics_history"):
            supabase.table("products_metrics_history").insert(structured_data["products_metrics_history"]).execute()
            
        if structured_data.get("reviews"):
            reviews = structured_data["reviews"]
            batch_size = 100
            for i in range(0, len(reviews), batch_size):
                batch = reviews[i:i+batch_size]
                supabase.table("reviews").upsert(batch).execute()

        logger.info(f"Upserted to Supabase successfully")
        
    except Exception as e:
        logger.error(f"Supabase upsert error: {e}")

async def get_product_links_from_supabase():
    if not supabase:
        logger.warning("Supabase client not available, skipping upsert")
        return []
    try:
        response = supabase.table("products").select("product_url").execute()
        return [row["product_url"] for row in response.data]
    except Exception as e:
        logger.error(f"Supabase upsert error: {e}")
        return []