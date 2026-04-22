import scrapy
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional
import random
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class BlibliSpiderJson(scrapy.Spider):
    name = 'blibli'
    BASE_URL = 'https://careers.blibli.com'
    API_URL = f'{BASE_URL}/ext/api/job/list?format=COMPLETE&groupBy=true'
    JOB_URL_TEMPLATE = f'{BASE_URL}/job-detail/{{}}'

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
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'referer': 'https://careers.blibli.com/department/all-departments',
            'user-agent': random.choice(self.USER_AGENTS)
        }
        yield scrapy.Request(self.API_URL, headers=headers, callback=self.parse)

    def parse(self, response):
        try:
            data = json.loads(response.text)
            departments = data.get('responseObject', [])

            if not departments:
                logger.info("Blibli: No departments found")
                return

            total = 0
            for department in departments:
                if isinstance(department, dict):
                    for job in department.get('jobs', []):
                        item = self.parse_job(job)
                        if item:
                            yield item
                            total += 1

            logger.info(f"Blibli: Scraped {total} jobs")

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)

    def parse_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.format_unix_time(job.get('createdDate'))

            job_title = self.sanitize_string(job.get('title'))
            department = self.sanitize_string(job.get('departmentName'))
            desc = f"{job_title} {department}"

            return {
                'job_title': job_title,
                'job_location': self.sanitize_string(job.get('location')),
                'job_department': department,
                'job_url': self.get_job_url(job),
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': self.get_employment_type(job),
                'job_level': self.sanitize_string(job.get('experience')),
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Blibli',
                'company_url': self.BASE_URL,
                'job_board': 'Blibli Job Portal',
                'job_board_url': 'https://careers.blibli.com',
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'On-site',
                'desc': desc
            }
        except Exception as e:
            logger.error(f"Blibli parse_job error: {e}")
            return None

    def get_job_url(self, job: Dict[str, Any]) -> str:
        job_title = job.get('title', '').lower().replace(' ', '-')
        job_id = job.get('id', '')
        return f"{self.BASE_URL}/job-detail/{job_title}?job={job_id}"

    @staticmethod
    def get_employment_type(job: Dict[str, Any]) -> str:
        employment_type = job.get('employmentType', '')
        return employment_type.replace("Ph-", "").replace("-", " ").capitalize() if employment_type else 'N/A'

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        return s.strip() if s else 'N/A'

    @staticmethod
    def format_unix_time(unix_time) -> str:
        if unix_time is None:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            return datetime.fromtimestamp(int(unix_time) / 1000).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")