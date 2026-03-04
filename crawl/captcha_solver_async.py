import os

import cv2
import numpy as np
import random
import asyncio
from playwright.async_api import Page

async def human_mouse_drag(page: Page, source_el, x_distance, y_offset=0):
    """
    Giả lập hành vi kéo chuột của con người với gia tốc và độ rung (Async version).
    """
    box = await source_el.bounding_box()
    start_x = box["x"] + box["width"] / 2
    start_y = box["y"] + box["height"] / 2

    await page.mouse.move(start_x, start_y)
    await asyncio.sleep(random.uniform(0.2, 0.4))
    await page.mouse.down()

    current_x = start_x
    target_x = start_x + x_distance
    steps = random.randint(30, 60)

    for i in range(steps):
        t = i / steps
        ease_t = t * (2 - t)

        next_x = start_x + (x_distance * ease_t)
        step_x = next_x - current_x

        jitter_y = random.uniform(-2, 2)

        await page.mouse.move(current_x + step_x, start_y + jitter_y)
        current_x += step_x

        await asyncio.sleep(random.uniform(0.005, 0.02))

    await page.mouse.move(target_x, start_y)
    await asyncio.sleep(random.uniform(0.1, 0.3))
    await page.mouse.up()


def find_puzzle_position(bg_img, piece_img):
    """Xử lý ảnh bằng OpenCV để tìm toạ độ lỗ hổng"""
    try:
        bg_gray = cv2.cvtColor(bg_img, cv2.COLOR_BGR2GRAY)
        piece_gray = cv2.cvtColor(piece_img, cv2.COLOR_BGR2GRAY)

        bg_edge = cv2.Canny(bg_gray, 100, 200)
        piece_edge = cv2.Canny(piece_gray, 100, 200)

        result = cv2.matchTemplate(bg_edge, piece_edge, cv2.TM_CCOEFF_NORMED)
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)

        return max_loc[0] 
    except Exception as e:
        print(f"[Solver] Lỗi xử lý ảnh: {e}")
        return None


async def solve_tiktok_captcha(page: Page):
    """
    Trả về:
    - "no_captcha": Nếu không phát hiện Captcha
    - "success": Nếu giải Captcha thành công
    - "failed": Nếu giải thất bại hoặc có lỗi
    """
    try:
        captcha_wrapper_sel = "#captcha_container, .captcha_verify_container"
        if await page.locator(captcha_wrapper_sel).count() == 0 and \
           await page.locator("#captcha-verify-image").count() == 0:
            return "no_captcha"

        bg_element = page.locator("#captcha-verify-image")
        piece_element = page.locator(".captcha_verify_img_slide")

        dir =  'dataset/images/train'

        bg_element.save(os.path.join(dir, "bg.png"))
        piece_element.save(os.path.join(dir, "piece.png"))

        bg_url = await bg_element.get_attribute("src")
        piece_url = await piece_element.get_attribute("src")

        if not bg_url or not piece_url:
            print("[Solver] Không lấy được URL ảnh.")
            return "failed"

        # Sử dụng request nội bộ của playwright thay vì requests đồng bộ
        r_bg = await page.context.request.get(bg_url)
        r_piece = await page.context.request.get(piece_url)
        
        bg_bytes = await r_bg.body()
        piece_bytes = await r_piece.body()
        

        bg_array = np.asarray(bytearray(bg_bytes), dtype=np.uint8)
        piece_array = np.asarray(bytearray(piece_bytes), dtype=np.uint8)

        bg_img = cv2.imdecode(bg_array, cv2.IMREAD_COLOR)
        piece_img = cv2.imdecode(piece_array, cv2.IMREAD_COLOR)

        raw_x = find_puzzle_position(bg_img, piece_img)
        if raw_x is None:
            return "failed"

        rendered_width = await bg_element.evaluate("el => el.clientWidth")
        natural_width = bg_img.shape[1]
        scale = rendered_width / natural_width
        final_distance = raw_x * scale

        slider_btn = page.locator(".secsdk-captcha-drag-icon").first
        if await slider_btn.count() == 0:
            slider_btn = page.locator(".secsdk-captcha-drag-icon")

        await human_mouse_drag(page, slider_btn, final_distance)
        await asyncio.sleep(2)

        if await page.locator("#captcha-verify-image").count() == 0:
            return "success"
        else:
            return "failed"

    except Exception as e:
        print(f"[Solver] Lỗi giải Captcha: {e}")
        return "failed"