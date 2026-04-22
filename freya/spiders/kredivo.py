import scrapy
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class KredivoSpider(scrapy.Spider):
    name = 'kredivo'
    BASE_URL = 'https://careers.kredivocorp.com'
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
            cards = response.css('ul.positions.location li.position')
            logger.info(f"Kredivo: Found {len(cards)} job cards")
            for job_card in cards:
                item = self.parse_job(job_card)
                if item:
                    yield item
        except Exception as e:
            logger.error(f"Kredivo parse error: {e}", exc_info=True)

    def parse_job(self, selector) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            job_title = self.sanitize_string(selector.css('h2::text').get(), is_title=True)
            job_location = self.sanitize_string(selector.css('li.location span::text').get())
            job_type = self.sanitize_string(selector.css('li.type span::text').get(), is_job_type=True)
            job_department = self.sanitize_string(selector.css('li.department span::text').get())

            href = selector.css('a::attr(href)').get() or ''
            job_url = self.BASE_URL + href if href else ''

            desc = f"{job_title} {job_department} {job_type}"

            return {
                'job_title': job_title,
                'job_location': job_location,
                'job_department': job_department,
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': job_type,
                'job_level': self.extract_job_level(job_title),
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Kredivo',
                'company_url': self.BASE_URL,
                'job_board': 'Kredivo Careers',
                'job_board_url': self.CAREERS_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'Remote' if 'Remote' in job_location else 'On-site',
                'desc': desc
            }
        except Exception as e:
            logger.error(f"Kredivo parse_job error: {e}")
            return None

    def extract_job_level(self, job_title: str) -> str:
        levels = ['Intern', 'Junior', 'Senior', 'Lead', 'Manager', 'Head', 'Director', 'VP', 'C-level']
        for level in levels:
            if level.lower() in job_title.lower():
                return level
        return 'N/A'

    @staticmethod
    def sanitize_string(s: Optional[str], is_title: bool = False, is_job_type: bool = False) -> str:
        if s is None:
            return 'N/A'
        s = s.strip()
        if is_title:
            s = s.replace(',', ' -')
        elif is_job_type:
            s = s.replace('%', '').strip()
            if 'LABEL_POSITION_TYPE' in s:
                s = s.split('LABEL_POSITION_TYPE_')[-1].strip()
        return ' '.join(s.split()) or 'N/A'