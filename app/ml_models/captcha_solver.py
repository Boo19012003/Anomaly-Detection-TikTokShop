import cv2
import numpy as np
import requests
import asyncio
import time
import random
import os
import logging
from playwright.async_api import TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
os.environ["YOLO_VERBOSE"] = "False"
from ultralytics import YOLO
logging.getLogger("ultralytics").setLevel(logging.WARNING)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CaptchaSolver")

def generate_tracks(distance):
    """Simulating humman mouse drag"""
    tracks = []
    current = 0
    steps = random.randint(25, 45)
    
    for i in range(1, steps + 1):
        t = i / steps
        ease_t = t * t * (3 - 2 * t)
        
        target = distance * ease_t
        step_move = target - current
        current = target
        
        track = round(step_move)
        if track > 0:
            tracks.append(track)
            
    track_sum = sum(tracks)
    if track_sum < distance:
        tracks.append(distance - track_sum)
    elif track_sum > distance:
        tracks.append(distance - track_sum)
        
    return tracks

async def human_mouse_drag(page, source_el, x_distance):
    box = await source_el.bounding_box()
    start_x = box["x"] + box["width"] / 2
    start_y = box["y"] + box["height"] / 2

    await page.mouse.move(start_x, start_y, steps=random.randint(5, 10))
    await asyncio.sleep(random.uniform(0.12, 0.24))
    await page.mouse.down()
    await asyncio.sleep(random.uniform(0.1, 0.2))

    tracks = generate_tracks(x_distance)
    
    current_x = start_x
    current_y = start_y

    for track in tracks:
        current_x += track
        y_wobble = current_y + random.uniform(-1.5, 1.5)
        
        await page.mouse.move(current_x, y_wobble)
        
        await asyncio.sleep(random.uniform(0.01, 0.03))

    target_x = start_x + x_distance

    if random.random() > 0.4:
        overshoot = random.uniform(2, 5)
        await page.mouse.move(target_x + overshoot, current_y + random.uniform(-1, 1), steps=2)
        await asyncio.sleep(random.uniform(0.05, 0.15))
        await page.mouse.move(target_x, current_y, steps=3)

    await page.mouse.move(target_x, current_y, steps=2)
    
    await asyncio.sleep(random.uniform(0.2, 0.4))
    await page.mouse.up()



current_dir = os.path.dirname(os.path.abspath(__file__))
_model_path = os.path.join(current_dir, "weights", "solver_captcha_tiktokshop.pt")

try:
    yolo_model = YOLO(_model_path)
except (ImportError, RuntimeError, OSError) as e:
    logger.error(f"Loading YOLO model failed: {e}")
    yolo_model = None


async def solve_tiktok_captcha(page):
    if yolo_model is None:
        logger.error("Cannot solve captcha because YOLO model is not loaded.")
        return "failed"

    bg_img = None
    try:
        captcha_wrapper_sel = "#captcha_container, .captcha_verify_container"
        wrapper_count = await page.locator(captcha_wrapper_sel).count()
        img_count = await page.locator("#captcha-verify-image").count()
        if wrapper_count == 0 and img_count == 0:
            return "no_captcha"

        bg_element = page.locator("#captcha-verify-image")
        bg_url = await bg_element.get_attribute("src")

        if not bg_url:
            logger.warning("Cannot get background image URL.")
            return "failed"

        response = await page.request.get(bg_url, timeout=5000)
        image_bytes = await response.body()
        bg_array = np.asarray(bytearray(image_bytes), dtype=np.uint8)
        bg_img = cv2.imdecode(bg_array, cv2.IMREAD_COLOR)

        results = await asyncio.to_thread(yolo_model, bg_img, verbose=False)
        boxes = results[0].boxes

        if len(boxes) == 0:
            logger.warning("YOLO cannot detect the hole in this image.")
            return "failed"

        best_box = boxes[0]
        x1, y1, x2, y2 = best_box.xyxy[0].tolist()
        raw_x = x1 

        rendered_width = await bg_element.evaluate("el => el.clientWidth")
        natural_width = bg_img.shape[1]
        scale = rendered_width / natural_width
        
        final_distance = raw_x * scale

        slider_btn = page.locator(".secsdk-captcha-drag-icon").first
        if await slider_btn.count() == 0:
            logger.warning("Not found slider button.")
            return "failed"
        
        await human_mouse_drag(page, slider_btn, final_distance)

        try:
            await page.wait_for_selector("#captcha-verify-image", state="hidden", timeout=2000)
        except PlaywrightTimeoutError:
            pass
            
        if await page.locator("#captcha-verify-image").count() == 0:
            return "success"
        else:
            return "failed"

    except PlaywrightTimeoutError as e:
        logger.error(f"Timeout solving captcha: {e}")
        return "failed"
    except PlaywrightError as e:
        logger.error(f"Playwright error in captcha solving process: {e}")
        return "failed"
    except cv2.error as e:
        logger.error(f"OpenCV error in captcha solving process: {e}")
        return "failed"
    except Exception as e:
        logger.error(f"Unexpected error in captcha solving process: {e}")
        return "failed"