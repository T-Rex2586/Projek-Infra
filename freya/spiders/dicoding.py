import scrapy
import re
from datetime import datetime


class DicodingSpider(scrapy.Spider):
    name = "dicoding"
    allowed_domains = ["dicoding.com"]
    start_urls = ["https://www.dicoding.com/academies/list"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 1,
    }

    def parse(self, response):
        cards = response.css("a[href*='/academies/']")

        seen_urls = set()
        for card in cards:
            url = card.attrib.get("href", "")
            if not re.search(r"/academies/\d+", url):
                continue
            if not url.startswith("http"):
                url = f"https://www.dicoding.com{url}"
            if url in seen_urls:
                continue
            seen_urls.add(url)

            title = (
                card.css("h5::text, .course-card__title::text, strong::text")
                .get(default="")
                .strip()
            )
            if not title:
                title = card.css("*::text").getall()
                title = next(
                    (t.strip() for t in title if len(t.strip()) > 10),
                    "N/A",
                )

            level = card.css(
                ".course-card__level::text, [class*='level']::text"
            ).get(default="N/A").strip()

            duration = card.css(
                ".course-card__duration::text, [class*='duration']::text"
            ).get(default="N/A").strip()

            rating = card.css(
                ".course-card__rating::text, [class*='rating']::text"
            ).get(default="N/A").strip()

            students = card.css(
                "[class*='student']::text, [class*='member']::text"
            ).get(default="N/A").strip()

            desc = card.css(
                ".course-card__desc::text, [class*='desc']::text, p::text"
            ).get(default="").strip()

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
