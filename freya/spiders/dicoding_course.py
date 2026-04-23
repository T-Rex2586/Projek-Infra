import scrapy
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class DicodingCourseSpider(scrapy.Spider):
    name = 'dicoding_course'
    BASE_URL = 'https://www.dicoding.com'
    START_URL = 'https://www.dicoding.com/academies/list'
    
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
            # Selector for Dicoding courses
            course_cards = response.css('.course-card')
            if not course_cards:
                # Alternate selector if they changed the UI
                course_cards = response.xpath('//div[contains(@class, "card")]//a[contains(@href, "/academies/")]')
                
            logger.info(f"Dicoding: Found {len(course_cards)} courses")

            for card in course_cards:
                item = self.parse_course(card)
                if item:
                    yield item

            # Handle pagination if exists
            next_page = response.css('li.next a::attr(href)').get()
            if next_page:
                yield scrapy.Request(response.urljoin(next_page), callback=self.parse)

        except Exception as e:
            logger.error(f"Error parsing Dicoding page: {e}", exc_info=True)

    def parse_course(self, selector) -> Optional[Dict[str, Any]]:
        try:
            course_title = self.sanitize_string(
                selector.css('h5.course-card__name::text').get() or 
                selector.xpath('.//h5/text()').get() or 
                selector.xpath('./text()').get()
            )
            
            href = selector.css('::attr(href)').get() or selector.xpath('./@href').get()
            course_url = self.BASE_URL + href if href and str(href).startswith('/') else href
            
            course_level = self.sanitize_string(
                selector.css('.course-card__level::text').get() or 
                selector.xpath('.//*[contains(@class, "level")]/text()').get()
            )
            
            if not course_title or course_title == 'N/A' or not course_url:
                return None

            return {
                'course_title': course_title,
                'provider': 'Dicoding',
                'course_url': course_url,
                'price': '0', # Default handling, usually needs detail parsing
                'course_level': course_level or 'All Levels',
                'instructor': 'Dicoding Expert',
                'scraped_at': self.timestamp,
                'is_course': True # Flag for ETL process
            }
        except Exception as e:
            logger.error(f"Dicoding parse_course error: {e}")
            return None

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        if not s: return 'N/A'
        return ' '.join(s.replace('\n', ' ').split())
