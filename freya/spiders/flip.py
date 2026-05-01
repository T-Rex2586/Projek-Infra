import scrapy
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class FlipSpider(scrapy.Spider):
    name = 'flip'
    BASE_URL = 'https://flip.id'
    CAREERS_URL = 'https://flip.id/careers'

    GREENHOUSE_URL = 'https://boards-api.greenhouse.io/v1/boards/flip/jobs?content=true'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1.0,
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
            self.GREENHOUSE_URL,
            headers={
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip, deflate',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            },
            callback=self.parse_greenhouse,
            errback=self.errback,
        )

    def parse_greenhouse(self, response):
        try:
            data = json.loads(response.body.decode('utf-8', errors='replace'))
            jobs = data.get('jobs', [])

            if not jobs:
                logger.warning(f"Flip Greenhouse: 0 jobs. Keys: {list(data.keys())}")
                return

            logger.info(f"Flip Greenhouse: Found {len(jobs)} jobs")
            for job in jobs:
                item = self.parse_job(job)
                if item:
                    yield item

        except json.JSONDecodeError as e:
            logger.error(f"Flip Greenhouse JSON error: {e}")
        except Exception as e:
            logger.error(f"Flip Greenhouse parse error: {e}", exc_info=True)

    def parse_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            updated_at = job.get('updated_at', '')
            last_seen = self.format_date(updated_at) if updated_at else first_seen

            job_title = self.sanitize(job.get('title', 'N/A'))

            loc_obj = job.get('location', {})
            location = loc_obj.get('name', 'Indonesia') if isinstance(loc_obj, dict) else 'Indonesia'

            depts = job.get('departments', [])
            department = depts[0].get('name', 'N/A') if depts and isinstance(depts[0], dict) else 'N/A'

            job_url = job.get('absolute_url', f"https://boards.greenhouse.io/flip/jobs/{job.get('id', '')}")

            content = job.get('content', '') or ''
            work_arr = 'Remote' if 'remote' in content.lower() else 'Hybrid' if 'hybrid' in content.lower() else 'On-site'

            desc = f"{job_title} {department} {content[:200]}"

            return {
                'job_title': job_title,
                'job_location': self.sanitize(location),
                'job_department': self.sanitize(department),
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': 'Full-time',
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Flip',
                'company_url': self.BASE_URL,
                'job_board': 'Flip Careers',
                'job_board_url': self.CAREERS_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': work_arr,
                'desc': desc,
            }
        except Exception as e:
            logger.error(f"Flip parse_job error: {e}")
            return None

    def errback(self, failure):
        logger.error(f"Flip request error: {failure.getErrorMessage()}")

    @staticmethod
    def sanitize(s: Optional[str]) -> str:
        return ' '.join(str(s).strip().split()) if s else 'N/A'

    @staticmethod
    def format_date(date_string: str) -> str:
        if not date_string:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
