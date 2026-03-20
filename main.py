import asyncio
from app.config.settings import get_logger
from app.crawler.producer import run_pipeline
from app.database.connection import get_supabase_client

logger = get_logger("MainApp")

async def main():
    logger.info("Connecting to Supabase")
    get_supabase_client()
    
    logger.info("Starting sequential execution pipeline")
    await run_pipeline()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped gracefully.")
