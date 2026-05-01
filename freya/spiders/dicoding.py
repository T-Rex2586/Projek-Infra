import scrapy
import json
from datetime import datetime


class DicodingSpider(scrapy.Spider):
    name = "dicoding"
    allowed_domains = ["dicoding.com"]

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS": 1,
        "ROBOTSTXT_OBEY": False,
    }

    def start_requests(self):
        yield scrapy.Request(
            url="https://www.dicoding.com/academies",
            meta={
                "playwright": True,
                "playwright_include_page": True,
            },
            callback=self.parse,
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]

        await page.wait_for_load_state("networkidle")

        content = await page.content()

        data = await page.evaluate("""
        () => {
            return window.__NEXT_DATA__;
        }
        """)

        await page.close()

        if not data:
            self.logger.error("Gagal ambil NEXT_DATA")
            return

        try:
            courses = data["props"]["pageProps"]["initialData"]["data"]
        except Exception as e:
            self.logger.error(f"Struktur berubah: {e}")
            return

        for course in courses:
            yield {
                "course_title": course.get("name", "N/A"),
                "desc": course.get("summary", "N/A"),
                "level": course.get("level", "N/A"),
                "duration": course.get("estimated_time", "N/A"),
                "rating": course.get("rating", "N/A"),
                "students": course.get("students_count", "N/A"),
                "platform": "Dicoding",
                "url": f"https://www.dicoding.com/academies/{course.get('id')}",
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }