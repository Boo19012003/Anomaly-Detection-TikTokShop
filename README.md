TikTokShop/
│
├── app/                        # Thư mục chứa toàn bộ mã nguồn chính
│   ├── __init__.py
│   ├── config/                 # Quản lý cấu hình
│   │   ├── __init__.py
│   │   └── settings.py         # Đọc biến từ .env, thiết lập USER_AGENTS, timeouts...
│   │
│   ├── crawler/                # Chịu trách nhiệm tương tác với trình duyệt (Extract)
│   │   ├── __init__.py
│   │   ├── browser.py          # Khởi tạo Playwright, chặn load ảnh/css (intercept_route)
│   │   ├── producer.py         # Lấy URL sản phẩm, đẩy vào Queue
│   │   └── consumer.py         # Nhận URL, cào chi tiết và gọi API
│   │
│   ├── ml_models/              # Chứa các model AI/ML
│   │   ├── __init__.py
│   │   ├── captcha_solver.py   # Code load model và inference
│   │   └── weights/            
│   │       └── solver_captcha_tiktokshop.pt  # Chuyển file .pt vào đây
│   │
│   ├── parser/                 # Chịu trách nhiệm làm sạch và bóc tách dữ liệu (Transform)
│   │   ├── __init__.py
│   │   ├── review_parser.py    # Bóc tách JSON API lấy thông tin review
│   │   └── nlp_utils.py        # Lọc spam, check độ dài, sentiment cơ bản
│   │
│   └── database/               # Chịu trách nhiệm lưu trữ (Load)
│       ├── __init__.py
│       ├── connection.py       # Kết nối DB
│       ├── crud.py             # Các hàm insert, update (ví dụ: chuyển logic save.py vào đây)
│       └── schema.sql          # Định nghĩa bảng
│
├── auth/                       # Chứa phiên đăng nhập
│   └── state.json              # (NHỚ THÊM VÀO .GITIGNORE)
│
├── data/                       # Chứa dữ liệu cục bộ (NHỚ THÊM VÀO .GITIGNORE)
│   ├── raw/                    # Thay thế cho response_web/ (chứa JSON/HTML thô)
│   └── processed/              # Chứa dữ liệu đã làm sạch (CSV, Parquet)
│
├── logs/                       # Nơi lưu file log của hệ thống (NHỚ THÊM VÀO .GITIGNORE)
│   └── crawler.log             
│
├── .env                        # Biến môi trường
├── .gitignore
├── README.md
├── requirements.txt
└── main.py                     # Entry point: Kết nối Producer, Consumer và DB