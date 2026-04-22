import scrapy
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional, List
import random
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class TiketSpiderJson(scrapy.Spider):
    name = 'tiket'
    BASE_URL = 'https://api.lever.co/v0/postings/tiket?mode=json'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1.0,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        },
    }

    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        headers = {
            'Accept': 'application/json',
            'Origin': 'https://careers.tiket.com',
            'Referer': 'https://careers.tiket.com/',
            'User-Agent': random.choice(self.USER_AGENTS),
        }
        yield scrapy.Request(self.BASE_URL, headers=headers, callback=self.parse)

    def parse(self, response):
        try:
            data = json.loads(response.text)
            if not isinstance(data, list):
                logger.error(f"Tiket: Expected list, got {type(data)}")
                return

            logger.info(f"Tiket: Found {len(data)} jobs")
            for job in data:
                item = self.parse_job(job)
                if item:
                    yield item
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)

    def parse_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.format_unix_time(job.get('createdAt'))

            categories = job.get('categories', {})
            if not isinstance(categories, dict):
                categories = {}

            job_title = job.get('text', 'N/A')
            department = categories.get('department', 'N/A')
            desc_plain = job.get('descriptionPlain', '')
            desc = f"{job_title} {department} {desc_plain}"

            return {
                'job_title': self.sanitize_string(job_title),
                'job_location': self.sanitize_string(categories.get('location')),
                'job_department': self.sanitize_string(department),
                'job_url': job.get('hostedUrl', ''),
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': self.sanitize_string(categories.get('commitment')),
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Tiket.com',
                'company_url': 'https://careers.tiket.com',
                'job_board': 'Tiket.com Careers',
                'job_board_url': 'https://careers.tiket.com',
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': job.get('workplaceType', 'On-site'),
                'desc': desc
            }
        except Exception as e:
            logger.error(f"Tiket parse_job error: {e}")
            return None

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        if s is None:
            return 'N/A'
        return s.replace(',', ' -').strip()

    @staticmethod
    def format_unix_time(unix_time) -> str:
        if unix_time is None:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            return datetime.fromtimestamp(int(unix_time) / 1000).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")