import cv2
import numpy as np
import requests
import time
import random
from playwright.sync_api import Page
from ultralytics import YOLO

def human_mouse_drag(page: Page, source_el, x_distance, y_offset=0):
    """
    Giả lập hành vi kéo chuột của con người với gia tốc và độ rung.
    """
    # Lấy vị trí bắt đầu (giữa nút kéo)
    box = source_el.bounding_box()
    start_x = box["x"] + box["width"] / 2
    start_y = box["y"] + box["height"] / 2

    # Di chuyển chuột đến vị trí bắt đầu
    page.mouse.move(start_x, start_y)
    time.sleep(random.uniform(0.2, 0.4))
    page.mouse.down()

    # Tạo lộ trình di chuyển
    current_x = start_x
    target_x = start_x + x_distance

    # Chia nhỏ quãng đường (30-60 bước)
    steps = random.randint(30, 60)

    for i in range(steps):
        t = i / steps
        # Hàm easing: easeOutQuad (nhanh đầu, chậm cuối)
        ease_t = t * (2 - t)

        next_x = start_x + (x_distance * ease_t)
        step_x = next_x - current_x

        # Thêm độ rung (jitter) trục Y
        jitter_y = random.uniform(-2, 2)

        page.mouse.move(current_x + step_x, start_y + jitter_y)
        current_x += step_x

        # Nghỉ siêu ngắn giữa các bước
        time.sleep(random.uniform(0.005, 0.02))

    # Đảm bảo đến đích chính xác
    page.mouse.move(target_x, start_y)
    time.sleep(random.uniform(0.1, 0.3))
    page.mouse.up()

# ==========================================
# Khởi tạo mô hình YOLO (Load 1 lần duy nhất)
# Hãy đảm bảo file best.pt nằm cùng thư mục với script này
# ==========================================
print("[System] Đang tải mô hình YOLOv8...")
try:
    yolo_model = YOLO('best.pt') 
    print("[System] Tải mô hình thành công!")
except Exception as e:
    print(f"[System] Lỗi tải mô hình YOLO: {e}")
    yolo_model = None


def solve_tiktok_captcha(page: Page):
    """
    Giải Slider Captcha bằng mô hình YOLOv8.
    Trả về: "no_captcha", "success", hoặc "failed"
    """
    if yolo_model is None:
        print("[Solver] Không thể giải Captcha vì mô hình YOLO chưa được tải.")
        return "failed"

    try:
        # 1. Kiểm tra sự xuất hiện của Captcha
        captcha_wrapper_sel = "#captcha_container, .captcha_verify_container"
        if page.locator(captcha_wrapper_sel).count() == 0 and \
           page.locator("#captcha-verify-image").count() == 0:
            return "no_captcha"

        # 2. Lấy URL và tải ảnh nền (chỉ cần ảnh nền)
        bg_element = page.locator("#captcha-verify-image")
        bg_url = bg_element.get_attribute("src")

        if not bg_url:
            print("[Solver] Không lấy được URL ảnh nền.")
            return "failed"

        r_bg = requests.get(bg_url, timeout=5)
        bg_array = np.asarray(bytearray(r_bg.content), dtype=np.uint8)
        bg_img = cv2.imdecode(bg_array, cv2.IMREAD_COLOR)

        # 3. Đưa ảnh vào mô hình YOLO để dự đoán
        # verbose=False để ẩn các log phân tích của YOLO trên console
        results = yolo_model(bg_img, verbose=False)
        boxes = results[0].boxes

        if len(boxes) == 0:
            print("[Solver] YOLO không nhận diện được lỗ hổng trên ảnh này.")
            return "failed"

        # Lấy toạ độ của bounding box có độ tin cậy cao nhất (YOLO tự sắp xếp giảm dần)
        best_box = boxes[0]
        x1, y1, x2, y2 = best_box.xyxy[0].tolist()
        
        # Toạ độ X đích chính là cạnh trái của bounding box
        raw_x = x1 

        # 4. Tính toán tỉ lệ (Scale Ratio) do ảnh hiển thị trên web bị thu nhỏ
        rendered_width = bg_element.evaluate("el => el.clientWidth")
        natural_width = bg_img.shape[1]
        scale = rendered_width / natural_width
        
        # Quãng đường chuột cần kéo
        final_distance = raw_x * scale

        # 5. Kéo thanh trượt
        slider_btn = page.locator(".secsdk-captcha-drag-icon").first
        if slider_btn.count() == 0:
             slider_btn = page.locator(".secsdk-captcha-drag-icon")

        print(f"[Solver] YOLO tìm thấy lỗ hổng tại X={raw_x:.1f}. Khoảng cách kéo: {final_distance:.1f}px")
        
        # Gọi hàm giả lập con người (giữ nguyên hàm human_mouse_drag cũ của bạn)
        human_mouse_drag(page, slider_btn, final_distance)

        # 6. Đợi và kiểm tra kết quả xác thực
        time.sleep(2)
        if page.locator("#captcha-verify-image").count() == 0:
            print("[Solver] Vượt Captcha THÀNH CÔNG!")
            return "success"
        else:
            print("[Solver] Vượt Captcha THẤT BẠI (Có thể do kéo lệch hoặc bị bắt bài).")
            return "failed"

    except Exception as e:
        print(f"[Solver] Lỗi trong quá trình giải Captcha: {e}")
        return "failed"