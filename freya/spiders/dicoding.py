import scrapy
from datetime import datetime


class DicodingSpider(scrapy.Spider):
    name = "dicoding"
    allowed_domains = ["dicoding.com"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 2,
    }

    BASE_URL = "https://www.dicoding.com/public/api/academies"

    def start_requests(self):
        # mulai dari page 1
        yield scrapy.Request(
            url=f"{self.BASE_URL}?page=1",
            headers=self.get_headers(),
            callback=self.parse,
            meta={"page": 1},
        )

    def get_headers(self):
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json",
            "Referer": "https://www.dicoding.com/",
        }

    def parse(self, response):
        page = response.meta.get("page", 1)

        try:
            data = response.json()
        except Exception:
            self.logger.error("Gagal parse JSON")
            return

        courses = data.get("data", [])

        if not courses:
            self.logger.info(f"Halaman {page} kosong, stop crawling")
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

        next_page = page + 1
        yield scrapy.Request(
            url=f"{self.BASE_URL}?page={next_page}",
            headers=self.get_headers(),
            callback=self.parse,
            meta={"page": next_page},
        )