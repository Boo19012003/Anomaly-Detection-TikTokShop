import os
import sys
import time
from playwright.sync_api import sync_playwright
from captcha_solver import solve_tiktok_captcha


def solve_captcha(page):
    captcha_count = 0
    max_retries = 5

    while captcha_count < max_retries:
        status = solve_tiktok_captcha(page)

        if status == "no_captcha":
            break
        elif status == "success":
            print("[Captcha Solver] Captcha đã được giải thành công.")
            time.sleep(2)
            break

        else:
            print(f"[Solve Captcha] Giải captcha thất bại, thử lại lần {captcha_count + 1}/{max_retries}")
            captcha_count += 1
            time.sleep(3)

            if captcha_count == max_retries:
                print("[Solve Captcha] Cảnh báo: Không thể giải captcha sau nhiều lần thử. Vui lòng kiểm tra thủ công.")

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

        solve_captcha(page)
        
        input("Nhấn Enter để đóng trình duyệt...")
        context.close()
        

if __name__ == "__main__":
    reviews()