import scrapy
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional
import random
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class SoftwareOneSpiderJson(scrapy.Spider):
    name = 'softwareone'
    BASE_URL = 'https://careers.softwareone.com'
    API_URL = f'{BASE_URL}/api/jobs?country=Indonesia&page=1&sortBy=posted_date&descending=true&internal=false&deviceId=undefined&domain=softwareone.jibeapply.com'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1.0,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy.core.downloader.handlers.http11.HTTP11DownloadHandler",
            "https": "scrapy.core.downloader.handlers.http11.HTTP11DownloadHandler",
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
            'accept': 'application/json',
            'referer': f'{self.BASE_URL}/en/jobs?country=Indonesia',
            'user-agent': random.choice(self.USER_AGENTS)
        }
        yield scrapy.Request(self.API_URL, headers=headers, callback=self.parse)

    def parse(self, response):
        try:
            data = json.loads(response.text)
            jobs = data.get('jobs', [])

            if not jobs:
                logger.info("SoftwareOne: No jobs found")
                return

            logger.info(f"SoftwareOne: Found {len(jobs)} jobs")
            for job in jobs:
                job_data = job.get('data', {})
                if job_data:
                    item = self.parse_job(job_data)
                    if item:
                        yield item

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)

    def parse_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.format_posted_date(job.get('posted_date', ''))

            job_title = job.get('title', 'N/A')
            description = job.get('description', '')
            qualifications = job.get('qualifications', '')
            tags = job.get('tags2', [])
            department = tags[0] if isinstance(tags, list) and tags else 'N/A'

            desc = f"{job_title} {department} {description} {qualifications}"

            return {
                'job_title': job_title,
                'job_location': job.get('full_location', 'N/A'),
                'job_department': department,
                'job_url': f"https://careers.softwareone.com/en/jobs/{job.get('req_id', '')}",
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': job.get('employment_type', 'N/A'),
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': job.get('hiring_organization', 'SoftwareOne'),
                'company_url': self.BASE_URL,
                'job_board': 'SoftwareOne Careers',
                'job_board_url': f'{self.BASE_URL}/en/jobs',
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': self.get_work_arrangement(job),
                'desc': desc
            }
        except Exception as e:
            logger.error(f"SoftwareOne parse_job error: {e}")
            return None

    def format_posted_date(self, date_string: str) -> str:
        if not date_string:
            return self.timestamp
        try:
            date_obj = datetime.strptime(date_string, "%B %d, %Y")
            return date_obj.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return self.timestamp

    def get_work_arrangement(self, job: Dict[str, Any]) -> str:
        title = job.get('title', '').lower()
        desc = job.get('description', '').lower()
        if 'remote' in title or 'remote' in desc:
            return 'Remote'
        elif 'hybrid' in title or 'hybrid' in desc:
            return 'Hybrid'
        return 'On-site'
