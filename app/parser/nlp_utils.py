import json
from math import pow
from datetime import datetime, timezone

async def parse_tiktok_description(raw_desc):
    if not raw_desc:
        return ""
    try:
        desc_blocks = json.loads(raw_desc)
        texts = [block.get("text", "").strip() for block in desc_blocks if block.get("type") == "text" and block.get("text", "").strip()]
        return " ".join(texts)
    except json.JSONDecodeError:
        return str(raw_desc).strip()

async def parse_number(text):
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
    now = datetime.now(timezone.utc)
    delta_t = (now - review_date).days
    return pow(0.5, delta_t / 30.0)
