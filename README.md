# TikTok Shop Market Tracker & Analyzer

Dự án này thu thập dữ liệu sản phẩm từ trang [TikTok Shop Việt Nam](https://www.tiktok.com/shop/vn), làm sạch và xử lý dữ liệu để phục vụ cho việc phân tích.

## Cách thức hoạt động

Dự án bao gồm hai phần chính:

1.  **Thu thập dữ liệu (`main.py`):**
    *   Sử dụng Playwright để khởi chạy một trình duyệt Chromium và tự động duyệt web.
    *   Truy cập vào trang chủ của TikTok Shop Việt Nam để lấy danh sách các danh mục sản phẩm.
    *   Với mỗi danh mục, kịch bản sẽ truy cập vào trang của danh mục đó, cuộn trang để tải thêm sản phẩm.
    *   Trích xuất thông tin chi tiết của từng sản phẩm, bao gồm: tên, nhãn (xu hướng, hàng Việt, deal), link, đánh giá, số lượng đã bán, giá gốc, giá hiện tại và phần trăm giảm giá.
    *   Lưu dữ liệu thô vào file `tiktok_shop_products.csv`. Kịch bản có sử dụng một hồ sơ người dùng trình duyệt (`tiktok_user_data`) để mô phỏng người dùng thật và tránh bị chặn.

## Cài đặt

1.  **Clone repository:**
    ```bash
    git clone https://github.com/Boo19012003/TikTokShop-Market-Tracker-Analyzer.git
    cd TikTokShop-Market-Tracker-Analyzer
    ```

2.  **Tạo môi trường ảo (khuyến nghị):**
    ```bash
    python -m venv .venv
    # Trên Windows
    .venv\Scripts\activate
    # Trên macOS/Linux
    source .venv/bin/activate
    ```

3.  **Cài đặt các gói phụ thuộc:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Cài đặt trình duyệt cho Playwright:**
    ```bash
    playwright install
    ```

## Sử dụng

### 1. Thu thập dữ liệu

Chạy file `main.py` để bắt đầu quá trình thu thập dữ liệu.
**Lưu ý:** Kịch bản sẽ mở một cửa sổ trình duyệt. Bạn có thể sẽ cần đăng nhập vào tài khoản TikTok của mình trong lần chạy đầu tiên để có thể truy cập đầy đủ vào các trang sản phẩm.

```bash
python main.py
```

Dữ liệu thô sẽ được lưu trong file `tiktok_shop_products.csv`.
