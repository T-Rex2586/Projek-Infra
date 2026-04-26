import scrapy
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)


class MekariSpider(scrapy.Spider):
    name = 'mekari'
    BASE_URL = 'https://mekari.com'
    CAREERS_URL = 'https://mekari.com/karir/'
    # Mekari gunakan Lever API
    LEVER_URL = 'https://api.lever.co/v0/postings/mekari?mode=json'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1.5,
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
            self.LEVER_URL,
            headers={
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            },
            callback=self.parse_lever,
            errback=self.errback_lever,
        )

    def errback_lever(self, failure):
        logger.warning(f"Mekari Lever failed: {failure}, trying Playwright")
        yield scrapy.Request(
            'https://mekari.hire.trakstar.com',
            meta={"playwright": True},
            callback=self.parse_playwright,
            errback=self.errback_playwright,
        )

    def parse_lever(self, response):
        """Parse Lever API for Mekari jobs."""
        try:
            data = json.loads(response.text)

            if not isinstance(data, list):
                logger.error(f"Mekari Lever: Expected list, got {type(data)}")
                yield scrapy.Request(
                    'https://mekari.hire.trakstar.com',
                    meta={"playwright": True},
                    callback=self.parse_playwright,
                )
                return

            logger.info(f"Mekari Lever: Found {len(data)} jobs")

            for job in data:
                item = self.parse_lever_job(job)
                if item:
                    yield item

        except json.JSONDecodeError as e:
            logger.error(f"Mekari Lever JSON error: {e}")
        except Exception as e:
            logger.error(f"Mekari Lever parse error: {e}", exc_info=True)

    def parse_lever_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.format_unix_time(job.get('createdAt'))

            categories = job.get('categories', {})
            if not isinstance(categories, dict):
                categories = {}

            job_title = self.sanitize_string(job.get('text', 'N/A'))
            department = self.sanitize_string(categories.get('department', 'N/A'))
            location = self.sanitize_string(categories.get('location', 'Indonesia'))
            commitment = self.sanitize_string(categories.get('commitment', 'Full-time'))
            team = self.sanitize_string(categories.get('team', ''))

            work_arr = 'Remote' if 'remote' in location.lower() else (
                'Hybrid' if 'hybrid' in location.lower() else 'On-site'
            )

            desc = f"{job_title} {department} {commitment} {team}"

            return {
                'job_title': job_title,
                'job_location': location,
                'job_department': department,
                'job_url': job.get('hostedUrl', ''),
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': commitment,
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Mekari',
                'company_url': self.BASE_URL,
                'job_board': 'Mekari Careers',
                'job_board_url': self.CAREERS_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': work_arr,
                'desc': desc,
            }
        except Exception as e:
            logger.error(f"Mekari parse_lever_job error: {e}")
            return None

    def parse_playwright(self, response):
        """Playwright fallback for Mekari trakstar page."""
        try:
            selectors = [
                'div.js-card.list-item',
                'div.job-card',
                'li.opening',
                'a[href*="job"]',
            ]

            cards = []
            for sel in selectors:
                cards = response.css(sel)
                if cards:
                    logger.info(f"Mekari Playwright: Found {len(cards)} with '{sel}'")
                    break

            total = 0
            for job in cards:
                item = self.parse_playwright_card(job)
                if item:
                    yield item
                    total += 1

            logger.info(f"Mekari Playwright: Scraped {total} jobs")

        except Exception as e:
            logger.error(f"Mekari Playwright parse error: {e}", exc_info=True)

    def parse_playwright_card(self, selector) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            job_title = (
                selector.css('h3.js-job-list-opening-name::text, h3::text, h2::text').get()
                or selector.css('::text').get()
                or 'N/A'
            )
            job_location = selector.css('div.js-job-list-opening-loc::text, .location::text').get() or 'Indonesia'
            job_department = selector.css('div.col-md-4 div.rb-text-4::text, .department::text').get() or 'N/A'
            job_type = selector.css('div.js-job-list-opening-meta span:first-child::text, .type::text').get() or 'Full-time'
            work_arr = selector.css('div.js-job-list-opening-meta span:last-child::text').get() or 'On-site'

            href = selector.css('a::attr(href)').get() or ''
            job_url = href if href.startswith('http') else ('https://mekari.hire.trakstar.com' + href if href else '')

            if not job_url:
                return None

            desc = f"{job_title} {job_department} {job_type}"

            return {
                'job_title': self.sanitize_string(job_title),
                'job_location': self.sanitize_string(job_location),
                'job_department': self.sanitize_string(job_department),
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': self.sanitize_string(job_type),
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Mekari',
                'company_url': self.BASE_URL,
                'job_board': 'Mekari Careers',
                'job_board_url': self.CAREERS_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': self.sanitize_string(work_arr),
                'desc': desc,
            }
        except Exception as e:
            logger.error(f"Mekari parse_playwright_card error: {e}")
            return None

    def errback_playwright(self, failure):
        logger.error(f"Mekari Playwright errback: {failure}")

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        if s:
            return ' '.join(s.strip().split()) or 'N/A'
        return 'N/A'

    @staticmethod
    def format_unix_time(unix_time) -> str:
        if unix_time is None:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            return datetime.fromtimestamp(int(unix_time) / 1000).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
