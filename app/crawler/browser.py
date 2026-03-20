import os
import random
import asyncio
from playwright.async_api import async_playwright
from app.ml_models.captcha_solver import solve_tiktok_captcha
from app.config.settings import USER_AGENTS, get_logger

logger = get_logger("BrowserUtils")

async def intercept_route(route):
    """
    Block load image/font, only allow API & HTML
    """
    url = route.request.url.lower()
    if "ibyteimg.com" in url and ("-origin-jpeg.jpeg" in url or "-origin-png.png" in url):
        await route.continue_()
        return

    if route.request.resource_type in ["image", "media", "font"]:
        await route.abort()
    else:
        await route.continue_()

async def check_captcha_visible(page):
    selectors = "#captcha_container, .captcha_verify_container, #tts_web_captcha_container"
    captcha_locators = page.locator(selectors)
    
    for i in range(await captcha_locators.count()):
        if await captcha_locators.nth(i).is_visible():
            return True
    return False

async def solve_captcha_async(page):
    captcha_count = 0
    max_retries = 10
    while captcha_count < max_retries:
        try:
            status = await solve_tiktok_captcha(page)
        except Exception as e:
            logger.warning(f"Captcha module error: {e}")
            break

        if status == "no_captcha":
            break
        elif status == "success":
            await asyncio.sleep(0.5)
            break
        else:
            captcha_count += 1
            logger.warning(f"Retry {captcha_count}/{max_retries}")
            await asyncio.sleep(1)

async def init_browser_context(playwright):
    args = [
        '--disable-blink-features=AutomationControlled',
        '--disable-infobars',
        '--no-sandbox'
    ]
    browser = await playwright.chromium.launch(
        headless=False,
        channel="chrome",
        args=args
    )
    
    state_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "auth", "state.json"))
    
    context_args = {
        "viewport": {"width": 1280, "height": 720},
        "user_agent": random.choice(USER_AGENTS)
    }
    
    if os.path.exists(state_path):
        context_args["storage_state"] = state_path
    else:
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        logger.warning(f"Not found storage_state at: {state_path}")
        
    context = await browser.new_context(**context_args)
    await context.route("**/*", intercept_route)
    return browser, context, context_args
