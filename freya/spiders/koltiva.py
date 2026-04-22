import scrapy
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class KoltivaSpider(scrapy.Spider):
    name = 'koltiva'
    BASE_URL = 'https://career.koltiva.com'
    API_URL = 'https://erp-api.koltitrace.com/api/v1/jobs?limit=100'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Origin': 'https://career.koltiva.com',
        }
        yield scrapy.Request(self.API_URL, headers=headers, callback=self.parse)

    def parse(self, response):
        try:
            data = json.loads(response.text)
            jobs_data = data.get('data', {}).get('data', [])

            logger.info(f"Koltiva: Found {len(jobs_data)} jobs")
            for job in jobs_data:
                item = self.parse_job(job)
                if item:
                    yield item
        except Exception as e:
            logger.error(f"Koltiva error: {e}", exc_info=True)

    def parse_job(self, job_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            position = job_data.get('position_name', 'N/A')
            section = job_data.get('unitsec_name', 'N/A')
            benefits = job_data.get('jobs_benefits_perks', '')
            desc = f"{position} {section} {benefits}"

            close_date = job_data.get('close_date', '')
            if close_date and 'T' in close_date:
                close_date = close_date.split('T')[0]

            return {
                'job_title': self.sanitize_string(position, is_title=True),
                'job_location': f"{self.sanitize_string(job_data.get('unit_name'))} - {self.sanitize_string(job_data.get('country_name'))}",
                'job_department': self.sanitize_string(section),
                'job_url': f"{self.BASE_URL}/list-job/{job_data.get('slug', '')}",
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': self.sanitize_string(job_data.get('work_period_name'), is_job_type=True),
                'job_level': self.sanitize_string(job_data.get('level_name')),
                'job_apply_end_date': close_date,
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Koltiva',
                'company_url': self.BASE_URL,
                'job_board': 'Koltiva Careers',
                'job_board_url': self.BASE_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'Remote' if 'Work-from-home' in str(benefits) else 'On-site',
                'desc': desc
            }
        except Exception as e:
            logger.error(f"Koltiva parse_job error: {e}")
            return None

    @staticmethod
    def sanitize_string(s: Optional[str], is_title: bool = False, is_job_type: bool = False) -> str:
        if s is None:
            return 'N/A'
        s = s.strip()
        s = s.replace(',', ' -')
        if is_title:
            s = s.title()
        elif is_job_type:
            s = s.replace('Contract', '').strip()
        return ' '.join(s.split()) or 'N/A'
