import scrapy
from datetime import datetime
import logging
from typing import Dict, Any, Optional
from scrapy_playwright.page import PageMethod

logger = logging.getLogger(__name__)

class SkilvulSpider(scrapy.Spider):
    name = 'skilvul'
    BASE_URL = 'https://skilvul.com'
    START_URL = 'https://skilvul.com/courses'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2.0,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        yield scrapy.Request(
            self.START_URL,
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
            course_cards = response.css('a[href^="/courses/"]')
            logger.info(f"Skilvul: Found {len(course_cards)} courses")

            seen_urls = set()
            for card in course_cards:
                href = card.css('::attr(href)').get()
                if not href or href in seen_urls:
                    continue
                    
                seen_urls.add(href)
                item = self.parse_course(card, href)
                if item:
                    yield item

        except Exception as e:
            logger.error(f"Error parsing Skilvul page: {e}", exc_info=True)

    def parse_course(self, selector, href) -> Optional[Dict[str, Any]]:
        try:
            texts = selector.css('::text').getall()
            texts = [t.strip() for t in texts if t.strip()]
            if not texts:
                return None
                
            course_title = texts[0]
            course_url = self.BASE_URL + href
            
            price_text = 'N/A'
            for t in texts:
                if 'Rp' in t or 'Gratis' in t:
                    price_text = t
                    break

            return {
                'course_title': course_title,
                'provider': 'Skilvul',
                'course_url': course_url,
                'price': price_text,
                'course_level': 'All Levels',
                'instructor': 'Skilvul Mentor',
                'scraped_at': self.timestamp,
                'is_course': True
            }
        except Exception as e:
            logger.error(f"Skilvul parse_course error: {e}")
            return None

    def errback(self, failure):
        logger.error(f"Skilvul request error: {failure}")
