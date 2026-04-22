import scrapy
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class FlipSpider(scrapy.Spider):
    name = 'flip'
    BASE_URL = 'https://career.flip.id'
    CAREERS_URL = f"{BASE_URL}/jobs"

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
            cards = response.css('div.job-list a.heading')
            logger.info(f"Flip: Found {len(cards)} job cards")
            for job_card in cards:
                item = self.parse_job(job_card)
                if item:
                    yield item
        except Exception as e:
            logger.error(f"Flip parse error: {e}", exc_info=True)

    def parse_job(self, selector) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            job_title = self.sanitize_string(selector.css('div.job-title::text').get())
            
            location_info = selector.css('div.location-info::text').get() or ''
            parts = [p.strip() for p in location_info.split('\n') if p.strip()]
            job_location = parts[0] if parts else 'N/A'
            job_type = parts[-1] if len(parts) > 1 else 'N/A'

            href = selector.css('::attr(href)').get() or ''
            job_url = self.BASE_URL + href if href else ''

            department = self.get_department(selector)
            desc = f"{job_title} {department} {job_type}"

            return {
                'job_title': job_title,
                'job_location': job_location,
                'job_department': department,
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': job_type,
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Flip',
                'company_url': self.BASE_URL,
                'job_board': 'Flip',
                'job_board_url': self.CAREERS_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'Remote' if 'Remote' in job_location else 'On-site',
                'desc': desc
            }
        except Exception as e:
            logger.error(f"Flip parse_job error: {e}")
            return None

    def get_department(self, selector) -> str:
        department = selector.xpath('ancestor::li/@data-portal-role').get()
        return department.replace('_role_', '') if department else 'N/A'

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        return ' '.join(s.strip().split()) if s else 'N/A'