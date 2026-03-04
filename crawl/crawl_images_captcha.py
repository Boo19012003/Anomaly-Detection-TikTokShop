import os
import time
from playwright.sync_api import sync_playwright
import requests
import supabase

def collect_captcha_images(page, save_dir):
    try:
        captcha_wrapper_sel = "#captcha_container, .captcha_verify_container"

        if page.locator(captcha_wrapper_sel).count() == 0 and page.locator("#captcha-verify-image").count() == 0:
            print("[Dataset] Không phát hiện Captcha.")
            return
        
        bg_element = page.locator("#captcha-verify-image")
        piece_element = page.locator(".captcha_verify_img_slide")

        bg_url = bg_element.get_attribute("src")
        piece_url = piece_element.get_attribute("src")

        if not bg_url or not piece_url:
            print("[Dataset] Không lấy được URL ảnh.")
            return
        
        response_bg = requests.get(bg_url, timeout=5)
        response_piece = requests.get(piece_url, timeout=5)

        if response_bg.status_code == 200 and response_piece.status_code == 200:
            timestamp = int(time.time() * 1000)
            bg_path = os.path.join(save_dir, f"bg_{timestamp}.jpg")
            piece_path = os.path.join(save_dir, f"piece_{timestamp}.jpg")

            with open(bg_path, "wb") as f:
                f.write(response_bg.content)
            with open(piece_path, "wb") as f:
                f.write(response_piece.content)

            print(f"[Dataset] Lưu ảnh thành công: {bg_path}, {piece_path}")
        else:
            print(f"[Dataset] Lỗi tải ảnh: BG status {response_bg.status_code}, Piece status {response_piece.status_code}")
    
    except Exception as e:
        print(f"[Dataset] Lỗi khi thu thập dữ liệu: {e}")


def reviews():
    with sync_playwright() as p:
        args = [
            '--disable-blink-features=AutomationControlled',
            '--start-maximized',
            '--disable-infobars',
            '--no-sandbox'
        ]

        context = p.chromium.launch_persistent_context(
            user_data_dir="reviews",
            headless=False,
            channel="chrome",
            args=args,
            viewport=None,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        page = context.pages[0]

        page.goto("https://www.tiktok.com/shop/vn")

        save_dir = "dataset/raw_images"
        os.makedirs(save_dir, exist_ok=True)

        for _ in range(200):
            collect_captcha_images(page, save_dir)
            page.locator("span:has-text('Refresh')").click(timeout=5000)

        # collect_captcha_images(page, save_dir)
        # page.locator("span:has-text('Refresh')").click(timeout=5000)

        # # if is_refresh:
        # #     print("[Dataset] Phát hiện nút Refresh")
        
        input("Nhấn Enter để đóng trình duyệt...")
        context.close()
        

if __name__ == "__main__":
    reviews()