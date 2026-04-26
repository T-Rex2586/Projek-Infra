import scrapy
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)


class VidioSpiderXPath(scrapy.Spider):
    name = 'vidio'
    BASE_URL = 'https://careers.vidio.com'
    CAREERS_URL = f"{BASE_URL}/careers"
    # Vidio uses Greenhouse
    GREENHOUSE_URL = 'https://api.greenhouse.io/v1/boards/vidio/jobs?content=true'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2.0,
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
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            },
            callback=self.parse_greenhouse,
            errback=self.errback_greenhouse,
        )

    def errback_greenhouse(self, failure):
        logger.warning(f"Vidio Greenhouse failed: {failure}, trying Playwright fallback")
        yield scrapy.Request(
            self.CAREERS_URL,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    scrapy.Request.__class__  # placeholder, see below
                ],
            },
            callback=self.parse_playwright,
        )

    def errback_greenhouse(self, failure):  # noqa: F811
        logger.warning(f"Vidio Greenhouse API failed: {failure}")
        yield scrapy.Request(
            self.CAREERS_URL,
            meta={"playwright": True},
            callback=self.parse_playwright,
        )

    def parse_greenhouse(self, response):
        """Parse Greenhouse JSON for Vidio jobs."""
        try:
            data = response.json()
            jobs = data.get('jobs', [])

            if not jobs:
                logger.info(f"Vidio Greenhouse: No jobs found, trying Playwright")
                yield scrapy.Request(
                    self.CAREERS_URL,
                    meta={"playwright": True},
                    callback=self.parse_playwright,
                )
                return

            logger.info(f"Vidio Greenhouse: Found {len(jobs)} jobs")

            for job in jobs:
                first_seen = self.timestamp
                last_seen = self.timestamp

                job_title = self.sanitize_string(job.get('title', 'N/A'))
                depts = job.get('departments', [{}])
                department = depts[0].get('name', 'N/A') if depts else 'N/A'
                offices = job.get('offices', [{}])
                location = offices[0].get('name', 'Indonesia') if offices else 'Indonesia'
                job_url = job.get('absolute_url', '')

                desc = f"{job_title} {department}"

                yield {
                    'job_title': job_title,
                    'job_location': self.sanitize_string(location),
                    'job_department': self.sanitize_string(department),
                    'job_url': job_url,
                    'first_seen': first_seen,
                    'base_salary': '0',
                    'job_type': 'Full-time',
                    'job_level': 'N/A',
                    'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                    'last_seen': last_seen,
                    'is_active': 'True',
                    'company': 'Vidio',
                    'company_url': self.BASE_URL,
                    'job_board': 'Vidio Careers',
                    'job_board_url': self.CAREERS_URL,
                    'job_age': calculate_job_age(first_seen, last_seen),
                    'work_arrangement': 'On-site',
                    'desc': desc,
                }
        except Exception as e:
            logger.error(f"Vidio Greenhouse parse error: {e}", exc_info=True)

    def parse_playwright(self, response):
        """Playwright fallback for Vidio careers page."""
        try:
            # Try multiple selectors - Vidio careers page structure
            selectors_to_try = [
                'div[class*="b-job"] a',
                'div.career-item a',
                'li.job-item a',
                'a[href*="/careers/"]',
                'div[class*="job"] h3',
            ]

            cards = []
            for sel in selectors_to_try:
                cards = response.css(sel)
                if cards:
                    logger.info(f"Vidio Playwright: Found {len(cards)} with '{sel}'")
                    break

            if not cards:
                # XPath fallback
                cards = response.xpath('//div[contains(@class, "job")]//a | //li[contains(@class, "job")]//a')
                logger.info(f"Vidio Playwright XPath: Found {len(cards)} elements")

            total = 0
            for selector in cards:
                item = self.parse_job(selector)
                if item:
                    yield item
                    total += 1

            logger.info(f"Vidio Playwright: Scraped {total} jobs")

        except Exception as e:
            logger.error(f"Vidio Playwright parse error: {e}", exc_info=True)

    def parse_job(self, selector) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            job_title = (
                selector.css('.b-job__name::text, .job-title::text, h3::text, h2::text').get()
                or selector.css('::text').get()
                or 'N/A'
            )
            job_location = selector.css('.b-job__location::text, .location::text, span::text').get() or 'Indonesia'
            job_department = selector.css('.b-job__department::text, .department::text').get() or 'N/A'

            href = selector.css('::attr(href), a::attr(href)').get() or ''
            job_url = href if href.startswith('http') else (self.BASE_URL + href if href else '')

            if not job_url or not job_title or job_title == 'N/A':
                return None

            desc = f"{job_title} {job_department}"

            return {
                'job_title': self.sanitize_string(job_title),
                'job_location': self.sanitize_string(job_location),
                'job_department': self.sanitize_string(job_department),
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': 'Full-time',
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Vidio',
                'company_url': self.BASE_URL,
                'job_board': 'Vidio Careers',
                'job_board_url': self.CAREERS_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'On-site',
                'desc': desc,
            }
        except Exception as e:
            logger.error(f"Vidio parse_job error: {e}")
            return None

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        return ' '.join(s.strip().split()) if s else 'N/A'