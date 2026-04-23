import scrapy
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class BwaCourseSpider(scrapy.Spider):
    name = 'buildwithangga'
    BASE_URL = 'https://buildwithangga.com'
    START_URL = 'https://buildwithangga.com/kelas'
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1.0,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        yield scrapy.Request(self.START_URL, callback=self.parse)

    def parse(self, response):
        try:
            course_cards = response.css('.course-card')
            if not course_cards:
                course_cards = response.xpath('//a[contains(@href, "/kelas/")]')

            logger.info(f"BWA: Found {len(course_cards)} courses")

            for card in course_cards:
                item = self.parse_course(card)
                if item:
                    yield item

            # BWA may use load more button or other pagination if needed
            # next_page handles ...

        except Exception as e:
            logger.error(f"Error parsing BWA page: {e}", exc_info=True)

    def parse_course(self, selector) -> Optional[Dict[str, Any]]:
        try:
            course_title = self.sanitize_string(
                selector.css('.course-title::text').get() or
                selector.xpath('.//*[contains(@class, "title")]/text()').get() or
                selector.xpath('.//h3/text()').get()
            )
            
            href = selector.css('::attr(href)').get() or selector.xpath('./@href').get()
            if not href or href == '#':
                # maybe it's not a link but a wrapper
                return None
                
            course_url = self.BASE_URL + href if str(href).startswith('/') else href
            
            price_text = self.sanitize_string(
                selector.css('.course-price::text').get() or 
                selector.xpath('.//*[contains(text(), "Rp")]/text()').get() or
                selector.xpath('.//*[contains(text(), "Gratis") or contains(text(), "Free")]/text()').get()
            )
            
            if not course_title or course_title == 'N/A':
                return None

            return {
                'course_title': course_title,
                'provider': 'BuildWithAngga',
                'course_url': course_url,
                'price': price_text,
                'course_level': 'All Levels',
                'instructor': 'BWA Mentor',
                'scraped_at': self.timestamp,
                'is_course': True # Flag for ETL process
            }
        except Exception as e:
            logger.error(f"BWA parse_course error: {e}")
            return None

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        if not s: return 'N/A'
        return ' '.join(s.replace('\n', ' ').split())
