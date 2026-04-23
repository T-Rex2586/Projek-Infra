# -*- coding: utf-8 -*-
import scrapy
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class GotoSpiderJson(scrapy.Spider):
    name = 'goto'
    base_url = 'https://api.lever.co/v0/postings/GoToGroup?mode=json'

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
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        }
        yield scrapy.Request(self.base_url, headers=headers)

    def parse(self, response):
        try:
            data = json.loads(response.text)

            total = 0
            # Lever API returns a list of job postings directly
            for job in data:
                item = self.parse_job(job)
                if item:
                    yield item
                    total += 1

            logger.info(f"GoTo: Scraped {total} jobs")

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)

    def parse_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            job_title = job.get('text', 'N/A').replace(',', '-')
            categories = job.get('categories', {})
            department = categories.get('department', 'N/A') if isinstance(categories, dict) else 'N/A'
            commitment = categories.get('commitment', 'N/A') if isinstance(categories, dict) else 'N/A'
            location = categories.get('location', 'N/A') if isinstance(categories, dict) else 'N/A'
            team = categories.get('team', 'N/A') if isinstance(categories, dict) else 'N/A'

            job_url = job.get('hostedUrl', '')

            desc = f"{job_title} {department} {commitment} {team}".strip()

            return {
                'job_title': job_title,
                'job_location': location,
                'job_department': department,
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': commitment,
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'GoTo',
                'company_url': 'https://www.gotocompany.com/careers',
                'job_board': 'GoTo Careers',
                'job_board_url': 'https://www.gotocompany.com/careers',
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'On-site',
                'desc': desc
            }
        except Exception as e:
            logger.error(f"GoTo parse_job error: {e}")
            return None
