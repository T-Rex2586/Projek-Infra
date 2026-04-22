# Scrapy settings for freya project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "freya"

SPIDER_MODULES = ["freya.spiders"]
NEWSPIDER_MODULE = "freya.spiders"

# ====================================================================
# 🔧 ANTI-BAN CONFIGURATION
# ====================================================================

# 🔥 WAJIB: Matikan robots.txt agar spider tidak diblokir
# Hampir semua job board memblokir bot via robots.txt
ROBOTSTXT_OBEY = False

# 🔥 User-Agent rotation - agar tidak terdeteksi sebagai bot
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0',
]

# ====================================================================
# 🕐 THROTTLE & DELAY
# ====================================================================

# Download delay antar request (dalam detik)
DOWNLOAD_DELAY = 1.5

# Concurrent requests per domain (jangan terlalu besar)
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4

# Enable AutoThrottle agar delay otomatis menyesuaikan
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

# ====================================================================
# 🔄 RETRY SETTINGS
# ====================================================================

RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# ====================================================================
# 🍪 COOKIES & HEADERS
# ====================================================================

COOKIES_ENABLED = True

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
}

# ====================================================================
# 📦 ITEM PIPELINES
# ====================================================================

ITEM_PIPELINES = {
    "freya.pipelines.FreyaPipeline": 300,
}

# ====================================================================
# 🌐 DOWNLOAD HANDLERS (Playwright)
# ====================================================================

# Enable Playwright download handlers
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# Playwright misc
PLAYWRIGHT_MAX_CONTEXTS = 8
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30 * 1000  # 30 seconds (increased)

# Launch in headless mode
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "args": ["--no-sandbox", "--disable-dev-shm-usage"],
}

# ====================================================================
# ⚙️ MISC
# ====================================================================

# Timeout for download (30 seconds)
DOWNLOAD_TIMEOUT = 30

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# Logging
LOG_LEVEL = "INFO"
