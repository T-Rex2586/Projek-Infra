import scrapy
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional
import random
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class KarirSpiderJson(scrapy.Spider):
    name = 'karir'
    BASE_URL = 'https://gateway2-beta.karir.com/v2/search/opportunities'
    LIMIT = 20  # Naikkan dari 10 ke 20
    MAX_OFFSET = 200  # Naikkan dari 100

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2.0,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy.core.downloader.handlers.http11.HTTP11DownloadHandler",
            "https": "scrapy.core.downloader.handlers.http11.HTTP11DownloadHandler",
        },
    }

    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://karir.com',
            'Referer': 'https://karir.com/',
            'User-Agent': random.choice(self.USER_AGENTS)
        }
        payload = self.get_payload(0)

        yield scrapy.Request(
            self.BASE_URL,
            method='POST',
            headers=headers,
            body=json.dumps(payload),
            callback=self.parse,
            dont_filter=True,
        )

    def get_payload(self, offset):
        return {
            "keyword": "data",
            "location_ids": [],
            "company_ids": [],
            "industry_ids": [],
            "job_function_ids": [],
            "degree_ids": [],
            "locale": "id",
            "limit": self.LIMIT,
            "offset": offset,
            "is_opportunity": True
        }

    def parse(self, response):
        try:
            data = json.loads(response.text)
            
            # Safe navigation
            data_obj = data.get('data', {})
            if not data_obj:
                logger.error(f"Karir: No 'data' in response. Keys: {list(data.keys())}")
                return

            opportunities = data_obj.get('opportunities', [])
            total_opportunities = data_obj.get('total_opportunities', 0)

            if not opportunities:
                logger.info("Karir: No opportunities found")
                return

            logger.info(f"Karir: Got {len(opportunities)} jobs (total: {total_opportunities})")

            for opp in opportunities:
                # Langsung parse dari search results tanpa detail page
                # Karena Next.js build ID berubah setiap deploy, detail page terlalu rapuh
                yield self.parse_job_from_search(opp)

            # Pagination
            current_offset = json.loads(response.request.body).get('offset', 0)
            next_offset = current_offset + self.LIMIT

            if next_offset < total_opportunities and next_offset < self.MAX_OFFSET:
                yield scrapy.Request(
                    self.BASE_URL,
                    method='POST',
                    headers={
                        'Accept': 'application/json, text/plain, */*',
                        'Content-Type': 'application/json',
                        'Origin': 'https://karir.com',
                        'Referer': 'https://karir.com/',
                        'User-Agent': random.choice(self.USER_AGENTS)
                    },
                    body=json.dumps(self.get_payload(next_offset)),
                    callback=self.parse,
                    dont_filter=True,
                )

        except Exception as e:
            logger.error(f"Error parsing Karir search: {e}", exc_info=True)

    def parse_job_from_search(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Parse job langsung dari search results tanpa ke detail page."""
        first_seen = self.timestamp
        last_seen = self.format_datetime(job.get('posted_at', ''))

        job_position = job.get('job_position', 'N/A')
        company_name = job.get('company_name', 'Unknown')
        location = job.get('location', 'Indonesia')
        
        # Extract info yang tersedia
        job_functions = job.get('job_functions', [])
        if isinstance(job_functions, list):
            department = ' - '.join(job_functions) if job_functions else 'N/A'
            functions_text = ' '.join(job_functions)
        else:
            department = str(job_functions) if job_functions else 'N/A'
            functions_text = str(job_functions)

        # Build desc from available data
        full_text_desc = f"{job_position} {functions_text} {company_name}"

        # Salary
        salary = '0'
        salary_lower = job.get('salary_lower')
        salary_upper = job.get('salary_upper')
        if salary_lower:
            salary = str(salary_lower)

        return {
            'job_title': self.sanitize_string(job_position),
            'job_location': self.sanitize_string(location),
            'job_department': department,
            'job_url': f"https://karir.com/opportunities/{job.get('id', '')}",
            'first_seen': first_seen,
            'base_salary': salary,
            'job_type': job.get('job_type', 'Full-time'),
            'job_level': ' - '.join(job.get('job_levels', [])) if isinstance(job.get('job_levels'), list) else 'N/A',
            'job_apply_end_date': self.format_datetime(job.get('expires_at', '')),
            'last_seen': last_seen,
            'is_active': 'True',
            'company': self.sanitize_string(company_name),
            'job_board': 'Karir.com',
            'job_board_url': 'https://karir.com/',
            'job_age': calculate_job_age(first_seen, last_seen) if last_seen else 'unknown',
            'work_arrangement': job.get('workplace', 'On-site'),
            'desc': full_text_desc
        }

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        if s is None: return 'N/A'
        return ' '.join(s.strip().replace(',', ' -').split())

    @staticmethod
    def format_datetime(date_string: str) -> str:
        if not date_string: return ""
        try:
            dt = datetime.strptime(date_string.split('.')[0], "%Y-%m-%dT%H:%M:%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return date_string

    def errback_http(self, failure):
        logger.error(f"Http Error: {failure}")