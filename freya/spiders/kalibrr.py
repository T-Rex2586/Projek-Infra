import scrapy
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date, strip_html

logger = logging.getLogger(__name__)

class KalibrrSpiderJson(scrapy.Spider):
    name = 'kalibrr'
    BASE_URL = 'https://www.kalibrr.com/kjs/job_board/search?limit=100&offset={}'
    JOB_URL_TEMPLATE = "https://www.kalibrr.com/c/{}/jobs/{}"
    COMPANY_URL_TEMPLATE = "https://www.kalibrr.com/id-ID/c/{}/jobs"
    JOB_BOARD_URL = 'https://www.kalibrr.com/id-ID/home'
    MAX_OFFSET = 1000

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1.0,
        # Fix: handle non-text response by not using Playwright for this
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy.core.downloader.handlers.http11.HTTP11DownloadHandler",
            "https": "scrapy.core.downloader.handlers.http11.HTTP11DownloadHandler",
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        yield scrapy.Request(
            self.BASE_URL.format(0),
            headers={
                'Accept': 'application/json',
                # ✅ Fix: exclude 'br' (Brotli) karena brotli package tidak terinstall
                # Server akan fallback ke gzip/deflate yang sudah didukung
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'en-US,en;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'Referer': 'https://www.kalibrr.com/id-ID/home',
            },
            callback=self.parse,
            errback=self.errback,
            dont_filter=True,
        )

    def parse(self, response):
        try:
            # Fix: response.text gagal karena content-type bukan text
            # Gunakan response.body decode manual
            raw_body = response.body.decode('utf-8', errors='replace')
            data = json.loads(raw_body)
            jobs = data.get('jobs', [])

            if not jobs:
                logger.info("Kalibrr: No more jobs found")
                return

            logger.info(f"Kalibrr: Got {len(jobs)} jobs")

            for job in jobs:
                item = self.parse_job(job)
                if item:
                    yield item

            # Pagination
            total_jobs = data.get('total', 0)
            current_offset = data.get('offset', 0)
            next_offset = current_offset + len(jobs)

            if next_offset < total_jobs and next_offset < self.MAX_OFFSET:
                logger.info(f"Kalibrr: Fetching offset {next_offset} (total: {total_jobs})")
                yield scrapy.Request(
                    self.BASE_URL.format(next_offset),
                    headers={
                        'Accept': 'application/json',
                        'Accept-Encoding': 'gzip, deflate',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Referer': 'https://www.kalibrr.com/id-ID/home',
                    },
                    callback=self.parse,
                    errback=self.errback,
                    dont_filter=True,
                )

        except Exception as e:
            logger.error(f"Error parsing Kalibrr: {e}", exc_info=True)

    def errback(self, failure):
        logger.error(f"Kalibrr request error: {failure.getErrorMessage()}")

    def parse_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.format_datetime(job.get('created_at', ''))

            # Gabungkan teks untuk skill extraction
            desc_raw = strip_html(job.get('description', ''))
            qual_raw = strip_html(job.get('qualifications', ''))
            desc_text = f"{job.get('name', '')} {job.get('function', '')} {desc_raw} {qual_raw}"

            company_obj = job.get('company', {})
            company_code = company_obj.get('code', '') if isinstance(company_obj, dict) else ''

            return {
                'job_title': self.sanitize_string(job.get('name', 'N/A')),
                'job_location': self.get_location(job.get('google_location')),
                'job_department': job.get('function', 'N/A'),
                'job_url': self.JOB_URL_TEMPLATE.format(company_code, job.get('id', '')),
                'first_seen': first_seen,
                'base_salary': str(job.get('base_salary', '0')) if job.get('base_salary') else '0',
                'job_type': job.get('tenure', 'N/A'),
                'job_level': self.get_job_level(job.get('education_level', 0)),
                'job_apply_end_date': self.format_datetime(job.get('application_end_date', '')),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': job.get('company_name', 'Unknown'),
                'company_url': self.COMPANY_URL_TEMPLATE.format(company_code),
                'job_board': 'Kalibrr',
                'job_board_url': self.JOB_BOARD_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': self.get_work_arrangement(job),
                'desc': desc_text
            }
        except Exception as e:
            logger.error(f"Kalibrr parse_job error: {e}")
            return None

    def get_work_arrangement(self, job: Dict[str, Any]) -> str:
        if job.get('is_work_from_home'):
            return 'Remote'
        elif job.get('is_hybrid'):
            return 'Hybrid'
        return 'On-site'

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        if not s: return 'N/A'
        return ' '.join(s.replace('\n', ' ').split())

    def get_location(self, google_location) -> str:
        if google_location and isinstance(google_location, dict):
            ac = google_location.get('address_components', {})
            if isinstance(ac, dict):
                city = ac.get('city', '')
                region = ac.get('region', '')
                return f"{city}, {region}".strip(', ') or 'N/A'
        return 'N/A'

    def get_job_level(self, education_level: int) -> str:
        education_map = {200: 'High School', 550: 'Bachelor', 650: 'Master'}
        return education_map.get(education_level, 'Junior/Mid-Level')

    def format_datetime(self, date_string: str) -> str:
        if not date_string: return self.timestamp
        try:
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return self.timestamp