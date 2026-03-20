from supabase import create_client, Client
from app.config.settings import SUPABASE_URL, SUPABASE_KEY, get_logger

logger = get_logger("DBConnection")

def get_supabase_client() -> Client:
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            client = create_client(SUPABASE_URL, SUPABASE_KEY)
            return client
        except Exception as e:
            logger.error(f"Supabase connection error: {e}")
    else:
        logger.warning("Missing Supabase credentials.")
    return None
