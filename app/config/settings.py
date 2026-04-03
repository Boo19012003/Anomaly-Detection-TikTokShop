import os
import sys
import logging
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ---------------------------------------------------------------------------
# InterceptHandler: Redirect all stdlib logging → loguru
# ---------------------------------------------------------------------------
class InterceptHandler(logging.Handler):
    """Captures logs from third-party libraries using stdlib logging
    and re-emits them through loguru so every log line flows through
    a single, unified pipeline."""

    def emit(self, record: logging.LogRecord) -> None:
        # Map stdlib level to loguru level
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find the caller frame outside of the logging module
        frame, depth = logging.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


# ---------------------------------------------------------------------------
# Suppress noisy third-party loggers
# ---------------------------------------------------------------------------
for _lib in ("httpx", "httpcore", "ultralytics", "playwright"):
    logging.getLogger(_lib).setLevel(logging.WARNING)

# Install the intercept handler on the root logger so every stdlib logger
# (including those from third-party packages) is redirected to loguru.
logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)

# ---------------------------------------------------------------------------
# Loguru sinks
# ---------------------------------------------------------------------------
logger.remove()  # Remove default stderr sink

# Sink 1 – Console (human-readable, colored)
logger.add(
    sys.stderr,
    level="INFO",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{extra[name]}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
    colorize=True,
)

# Sink 2 – JSON file (structured, rotated, async-safe)
logger.add(
    "logs/tiktokshop_pipeline_{time:YYYY-MM-DD}.json",
    level="DEBUG",
    serialize=True,           # JSON output
    rotation="100 MB",        # New file after 100 MB
    retention="7 days",       # Auto-cleanup after 7 days
    enqueue=True,             # Thread/async-safe via internal queue
    encoding="utf-8",
)

# Provide a default value for the 'name' extra key so sinks that
# reference {extra[name]} never raise a KeyError.
logger.configure(extra={"name": "root"})


def get_logger(name):
    """Return a loguru logger bound with a contextual *name* field."""
    return logger.bind(name=name)

# Browser settings
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

TIMEOUT = 30000
MAX_CONCURRENT_PAGES = int(os.getenv("MAX_CONCURRENT_PAGES", "3"))
UPSERT_BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "50"))
UPSERT_FLUSH_INTERVAL = int(os.getenv("UPSERT_FLUSH_INTERVAL", "10"))

# Database settings
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
