import scrapy
from datetime import datetime
import logging
from typing import Dict, Any, Optional
from scrapy_playwright.page import PageMethod

logger = logging.getLogger(__name__)

class MyskillSpider(scrapy.Spider):
    name = 'myskill'
    BASE_URL = 'https://myskill.id'
    START_URLS = [
        'https://myskill.id/bootcamp',
        'https://myskill.id/course'
    ]

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2.0,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        for url in self.START_URLS:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_timeout", 5000)
                    ]
                },
                callback=self.parse,
                errback=self.errback
            )

    def parse(self, response):
        try:
            course_cards = response.css('a[href*="/bootcamp/"], a[href*="/course/"]')
            logger.info(f"MySkill: Found {len(course_cards)} courses on {response.url}")

            seen_urls = set()
            for card in course_cards:
                href = card.css('::attr(href)').get()
                if not href or href in seen_urls or href == '/bootcamp' or href == '/course':
                    continue
                    
                seen_urls.add(href)
                item = self.parse_course(card, href)
                if item:
                    yield item

        except Exception as e:
            logger.error(f"Error parsing MySkill page: {e}", exc_info=True)

    def parse_course(self, selector, href) -> Optional[Dict[str, Any]]:
        try:
            texts = selector.css('::text').getall()
            texts = [t.strip() for t in texts if t.strip()]
            if not texts:
                return None
                
            # Filter text to find title (usually the longest or first meaningful text)
            course_title = texts[0]
            for t in texts:
                if len(t) > 10 and 'Rp' not in t and 'Diskon' not in t:
                    course_title = t
                    break
                    
            course_url = self.BASE_URL + href if href.startswith('/') else href
            
            price_text = 'N/A'
            for t in texts:
                if 'Rp' in t or 'Gratis' in t or 'Free' in t:
                    price_text = t
                    break

            return {
                'course_title': course_title,
                'provider': 'MySkill',
                'course_url': course_url,
                'price': price_text,
                'course_level': 'All Levels',
                'instructor': 'MySkill Mentor',
                'scraped_at': self.timestamp,
                'is_course': True
            }
        except Exception as e:
            logger.error(f"MySkill parse_course error: {e}")
            return None

    def errback(self, failure):
        logger.error(f"MySkill request error: {failure}")
