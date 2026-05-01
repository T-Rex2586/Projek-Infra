import scrapy
import re
from datetime import datetime


class DicodingSpider(scrapy.Spider):
    name = "dicoding"
    allowed_domains = ["dicoding.com"]

    handle_httpstatus_list = [405]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS": 1,
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
    }

    async def start(self):
        yield scrapy.Request(
            url="https://www.dicoding.com/academies/list",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
            meta={
                "playwright": True,
                "playwright_include_page": True,
            },
            callback=self.parse,
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]

        # tunggu card muncul
        await page.wait_for_selector("a[href*='/academies/']", timeout=15000)

        content = await page.content()
        await page.close()

        response = response.replace(body=content)

        seen_urls = set()

        for card in response.css("a[href*='/academies/']"):
            url = card.attrib.get("href", "")

            if not re.search(r"/academies/\d+", url):
                continue

            if not url.startswith("http"):
                url = f"https://www.dicoding.com{url}"

            if url in seen_urls:
                continue
            seen_urls.add(url)

            title = (
                card.css("h5::text, strong::text")
                .get(default="")
                .strip()
            )

            if not title:
                title = "N/A"

            level = card.css("[class*='level']::text").get(default="N/A").strip()
            duration = card.css("[class*='duration']::text").get(default="N/A").strip()
            rating = card.css("[class*='rating']::text").get(default="N/A").strip()
            students = card.css("[class*='student']::text").get(default="N/A").strip()
            desc = card.css("p::text").get(default="").strip()

            yield {
                "course_title": title,
                "desc": desc,
                "level": level,
                "duration": duration,
                "rating": rating,
                "students": students,
                "platform": "Dicoding",
                "url": url,
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }