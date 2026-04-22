import scrapy
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional
import random
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class DeallsSpiderJson(scrapy.Spider):
    name = 'dealls'
    BASE_URL = 'https://api.sejutacita.id/v1/explore-job/job'
    JOBS_PER_PAGE = 18
    MAX_PAGES = 20  # Naikkan dari 5 ke 20

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1.5,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        },
    }

    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
    ]

    @classmethod
    def get_random_user_agent(cls):
        return random.choice(cls.USER_AGENTS)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://dealls.com',
            'referer': 'https://dealls.com/',
            'user-agent': self.get_random_user_agent()
        }
        url = f"{self.get_paginated_url(1)}&search=data"
        yield scrapy.Request(url, headers=headers, callback=self.parse)

    def get_paginated_url(self, page: int) -> str:
        return f"{self.BASE_URL}?page={page}&sortParam=mostRelevant&sortBy=asc&published=true&limit={self.JOBS_PER_PAGE}&status=active"

    def parse(self, response):
        try:
            data = json.loads(response.text)
            data_obj = data.get('data', {})
            if not data_obj:
                logger.error(f"Dealls: No 'data' key. Keys: {list(data.keys())}")
                return

            jobs = data_obj.get('docs', [])

            if not jobs:
                logger.info("Dealls: No more jobs")
                return

            for job in jobs:
                item = self.parse_job(job)
                if item:
                    yield item

            current_page = data_obj.get('page', 1)
            total_pages = data_obj.get('totalPages', 1)

            logger.info(f"Dealls: Page {current_page}/{total_pages}, got {len(jobs)} jobs")

            if current_page < total_pages and current_page < self.MAX_PAGES:
                next_page = current_page + 1
                next_page_url = f"{self.get_paginated_url(next_page)}&search=data"
                yield scrapy.Request(
                    next_page_url,
                    headers={
                        'accept': 'application/json, text/plain, */*',
                        'origin': 'https://dealls.com',
                        'referer': 'https://dealls.com/',
                        'user-agent': self.get_random_user_agent()
                    },
                    callback=self.parse
                )

        except Exception as e:
            logger.error(f"Error parsing Dealls JSON: {e}", exc_info=True)

    def parse_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp

            last_seen_raw = job.get('latestUpdatedAt') or job.get('createdAt', '')
            last_seen = self.format_datetime(last_seen_raw) if last_seen_raw else first_seen

            # Gabungkan teks untuk skill extraction
            full_desc = f"{job.get('role', '')} {job.get('description', '')}"

            city_obj = job.get('city', {})
            city_name = city_obj.get('name', 'N/A') if isinstance(city_obj, dict) else 'N/A'
            country_obj = job.get('country', {})
            country_name = country_obj.get('name', 'Indonesia') if isinstance(country_obj, dict) else 'Indonesia'

            company_obj = job.get('company', {})
            company_name = company_obj.get('name', 'Unknown') if isinstance(company_obj, dict) else 'Unknown'
            company_slug = company_obj.get('slug', '') if isinstance(company_obj, dict) else ''

            employment_types = job.get('employmentTypes', [])
            job_type = ', '.join(employment_types) if isinstance(employment_types, list) and employment_types else 'N/A'

            return {
                'job_title': self.sanitize_string(job.get('role', 'N/A')),
                'job_location': f"{city_name}, {country_name}",
                'job_department': self.get_job_department(job),
                'job_url': f"https://dealls.com/role/{job.get('slug', '')}",
                'first_seen': first_seen,
                'base_salary': self.get_job_salary(job),
                'job_type': job_type,
                'job_level': self.get_job_level(job),
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True' if job.get('status') == 'active' else 'False',
                'company': company_name,
                'company_url': f"https://dealls.com/company/{company_slug}",
                'job_board': 'Dealls',
                'job_board_url': 'https://dealls.com/',
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': str(job.get('workplaceType', 'On-site')).capitalize(),
                'desc': full_desc
            }
        except Exception as e:
            logger.error(f"Dealls parse_job error: {e}")
            return None

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        if not s: return 'N/A'
        return ' '.join(s.replace(", ", " - ").split())

    @staticmethod
    def get_job_department(job: Dict[str, Any]) -> str:
        department = job.get('categorySlug')
        return department.replace("-", " ").title() if department else 'N/A'

    @staticmethod
    def get_job_salary(job: Dict[str, Any]) -> str:
        salary_range = job.get('salaryRange')
        if isinstance(salary_range, dict) and salary_range.get('start'):
            return str(salary_range['start'])
        return '0'

    def get_job_level(self, job: Dict[str, Any]) -> str:
        try:
            cp = job.get('candidatePreference')
            if isinstance(cp, dict):
                educations = cp.get('lastEducations', [])
                if isinstance(educations, list):
                    if 7 in educations: return "Postgraduate"
                    if 6 in educations: return "Graduate"
                    if 5 in educations: return "Undergraduate"
        except:
            pass
        return "Junior/Mid-Level"

    @staticmethod
    def format_datetime(date_string: str) -> str:
        if not date_string: return ""
        try:
            dt = datetime.strptime(date_string.split('.')[0], "%Y-%m-%dT%H:%M:%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return date_string