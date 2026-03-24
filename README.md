# TikTokShop Data Pipeline & Crawler

A highly scalable, asynchronous web scraping and data processing pipeline for TikTok Shop. Built with Python, Playwright, and Supabase, this system is capable of systematically extracting product details, user reviews, and timeseries data while automatically bypassing captchas using Computer Vision.

## 🚀 Key Features

- **Asynchronous Data Collection**: Utilizes `asyncio` and `playwright` to process multiple URLs concurrently.
- **Automated Captcha Solving**: Integrates `ultralytics` YOLO models and `opencv` to automatically detect and solve TikTok's puzzles/captchas mimicking human mouse movement.
- **Robust Stateful Pipelines**: Splits workflows into three discrete, resumable pipelines (`Products`, `Reviews`, `Timeseries`).
- **Cloud-Ready Logging**: Extensive standard logging configuration designed to be caught by cloud platforms (AWS CloudWatch, GCP Logging, etc.) for job schedulers.
- **Supabase Integration**: Automatically upserts cleaned, structured data into PostgreSQL via the Supabase client.

## 📁 Project Structure

```text
TikTokShop/
├── app/                        
│   ├── config/                 # Environment configurations (settings.py)
│   ├── crawler/                # Playwright interactions & data crawler logic
│   ├── database/               # Supabase CRUD operations
│   ├── ml_models/              # Vision models for captcha bypassing
│   └── parser/                 # Regex & NLP utilities for HTML to JSON data extraction
├── pipeline/                   # Executable scripts for different data layers 
│   ├── products.py             # Scrapes product metadata from category URLs
│   ├── review.py               # Scrapes detailed user reviews for collected products
│   └── timeseries.py           # Extracts daily/timeseries metrics periodically
├── auth/                       # Browser authentication states (git-ignored)
├── logs/                       # Rotating local run logs (git-ignored)
├── .env.example                # Example environment variables required
├── README.md
└── requirements.txt
```

## 🛠️ Prerequisites

- Python 3.9+
- [Node.js](https://nodejs.org/) (Optional: required for `gitnexus` code indexing)

## 📦 Installation

1. **Clone the repository:**
   ```bash
   git clone <repository_url>
   cd TikTokShop
   ```

2. **Set up a virtual environment (Recommended):**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

4. **Environment Variables:**
   Create a `.env` file at the root of the project with the following configuration:
   ```ini
   SUPABASE_URL="https://your-project-id.supabase.co"
   SUPABASE_KEY="your-anon-or-service-role-key"
   ```

## 🏃 Usage

This project separates data harvesting tasks into independent pipelines. These scripts can be run locally or configured as Cron jobs/Airflow tasks in the cloud.

1. **Product Pipeline** (`pipeline/products.py`):
   Crawls targeted categories to find product URLs and basic product metadata.
   ```bash
   python pipeline/products.py
   ```

2. **Review Pipeline** (`pipeline/review.py`):
   Processes previously crawled products to extract recent reviews and ratings. Bypasses review pagination intelligently.
   ```bash
   python pipeline/review.py
   ```

3. **Timeseries Pipeline** (`pipeline/timeseries.py`):
   Designed for periodic execution to track metrics, sales, and prices over time without downloading heavy constant details.
   ```bash
   python pipeline/timeseries.py
   ```

## 🛡️ License
Proprietary/Internal System - Internal use only.