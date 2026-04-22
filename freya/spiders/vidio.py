import scrapy
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class VidioSpiderXPath(scrapy.Spider):
    name = 'vidio'
    BASE_URL = 'https://careers.vidio.com'
    CAREERS_URL = f"{BASE_URL}/careers"

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        yield scrapy.Request(self.CAREERS_URL, meta={"playwright": True}, callback=self.parse)

    def parse(self, response):
        try:
            selectors = response.xpath('//div[contains(@class, "b-job")]/a')
            logger.info(f"Vidio: Found {len(selectors)} job cards")

            for selector in selectors:
                item = self.parse_job(selector)
                if item:
                    yield item

            # Handle pagination
            next_page_url = response.xpath('//li[@class="next"]/a/@href').get()
            if next_page_url:
                yield scrapy.Request(
                    response.urljoin(next_page_url),
                    meta={"playwright": True},
                    callback=self.parse
                )

        except Exception as e:
            logger.error(f"Error parsing page: {e}", exc_info=True)

    def parse_job(self, selector) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            job_title = self.sanitize_string(selector.css('.b-job__name::text').get())
            job_location = self.sanitize_string(selector.css('.b-job__location::text').get())
            job_department = self.sanitize_string(selector.css('.b-job__department::text').get())

            href = selector.css('::attr(href)').get() or ''
            job_url = self.BASE_URL + href if href else ''

            desc = f"{job_title} {job_department}"

            return {
                'job_title': job_title,
                'job_location': job_location,
                'job_department': job_department,
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': 'N/A',
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Vidio',
                'company_url': self.BASE_URL,
                'job_board': 'Vidio',
                'job_board_url': self.CAREERS_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'On-site',
                'desc': desc
            }
        except Exception as e:
            logger.error(f"Vidio parse_job error: {e}")
            return None

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        return s.strip() if s else 'N/A'