import scrapy
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date, strip_html

logger = logging.getLogger(__name__)

class KitaLulusSpider(scrapy.Spider):
    name = 'kitalulus'
    BASE_URL = 'https://kitalulus.com'
    # Use API or fallback to page scraping logic
    # Real KitaLulus might need Playwright or GraphQL. Here we use an API mock pattern
    API_URL = 'https://api.kitalulus.com/v1/jobs?limit=50&page={}'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1.0,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        # We will use playwright as fallback since API access might be restricted
        yield scrapy.Request(
            'https://kitalulus.com/lowongan-kerja?categories=IT+dan+Software',
            meta={"playwright": True},
            callback=self.parse
        )

    def parse(self, response):
        try:
            # We mock the parsing logic for KitaLulus jobs UI
            selectors = response.xpath('//a[contains(@href, "/lowongan-kerja/")]')
            logger.info(f"KitaLulus: Found {len(selectors)} job cards")

            for selector in selectors:
                item = self.parse_job(selector)
                if item:
                    yield item

        except Exception as e:
            logger.error(f"Error parsing page KitaLulus: {e}", exc_info=True)

    def parse_job(self, selector) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            # These are generic selectors based on typical job boards
            job_title = self.sanitize_string(selector.xpath('.//h3/text()').get())
            company = self.sanitize_string(selector.xpath('.//p[contains(@class, "company")]/text()').get() or 'Unknown Company')
            job_location = self.sanitize_string(selector.xpath('.//span[contains(@class, "location")]/text()').get() or 'Indonesia')
            
            href = selector.xpath('./@href').get() or ''
            job_url = self.BASE_URL + href if href.startswith('/') else href

            if not job_title or job_title == 'N/A' or not job_url:
                return None

            desc = f"{job_title} at {company}"

            return {
                'job_title': job_title,
                'job_location': job_location,
                'job_department': 'IT & Software',
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': 'Full-time',
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': company,
                'company_url': self.BASE_URL,
                'job_board': 'KitaLulus',
                'job_board_url': self.BASE_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'On-site',
                'desc': desc
            }
        except Exception as e:
            logger.error(f"KitaLulus parse_job error: {e}")
            return None

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        if not s: return 'N/A'
        return ' '.join(s.replace('\n', ' ').split())
