import scrapy
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class EvermosSpiderJson(scrapy.Spider):
    name = 'evermos'
    BASE_URL = 'https://evermos-talent.freshteam.com/hire/widgets/jobs.json'
    COMPANY_URL = 'https://evermos.com/'

    JOB_TYPE_MAPPING = {
        1: "Contract",
        2: "Full Time",
        3: "Intern"
    }

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
        headers = self.get_headers()
        yield scrapy.Request(self.BASE_URL, headers=headers, callback=self.parse)

    def get_headers(self):
        return {
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'User-Agent': 'Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        }

    def parse(self, response):
        try:
            data = json.loads(response.text)
            branch_lookup = {branch['id']: branch.get('city', 'N/A') for branch in data.get('branches', [])}
            job_roles_lookup = {jr['id']: jr.get('name', 'N/A') for jr in data.get('job_roles', [])}

            jobs = data.get('jobs', [])
            logger.info(f"Evermos: Found {len(jobs)} jobs")

            for job in jobs:
                item = self.parse_job(job, branch_lookup, job_roles_lookup)
                if item:
                    yield item
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)

    def parse_job(self, job: Dict[str, Any], branch_lookup: Dict, job_roles_lookup: Dict) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.format_datetime(job.get('created_at', ''))

            job_title = self.sanitize_string(job.get('title'))
            department = job_roles_lookup.get(job.get('job_role_id'), 'N/A')
            description = job.get('description', '')
            desc = f"{job_title} {department} {description}"

            remote = job.get('remote', False)
            if remote:
                work_arrangement = 'Remote'
            else:
                work_arrangement = 'On-site'

            return {
                'job_title': job_title,
                'job_location': branch_lookup.get(job.get('branch_id'), 'N/A'),
                'job_department': department,
                'job_url': job.get('url', ''),
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': self.get_job_type(job.get('job_type', 0)),
                'job_level': job.get('position_level', 'N/A'),
                'job_apply_end_date': job.get('closing_date', ''),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Evermos',
                'company_url': self.COMPANY_URL,
                'job_board': 'Evermos Careers',
                'job_board_url': 'https://evermos.com/home/karir/',
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': work_arrangement,
                'desc': desc
            }
        except Exception as e:
            logger.error(f"Evermos parse_job error: {e}")
            return None

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        return s.replace(", ", " - ") if s else 'N/A'

    def get_job_type(self, job_type) -> str:
        return self.JOB_TYPE_MAPPING.get(job_type, "N/A")

    def format_datetime(self, date_string: str) -> str:
        if not date_string:
            return self.timestamp
        try:
            dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                return self.timestamp
