import scrapy
import json
from datetime import datetime
import logging
import re
from typing import Dict, Any, Optional
from scrapy_playwright.page import PageMethod
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class GlintsSpider(scrapy.Spider):
    name = 'glints'
    BASE_URL = 'https://glints.com'
    # Gunakan URL halaman biasa, Playwright akan render JS
    SEARCH_URL = '{base}/id/opportunities/jobs/explore?keyword=data&country=ID&locationName=All+Cities%2FProvinces&lopiOnly=true&sortBy=LATEST&page={page}'

    MAX_PAGES = 10

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 3.0,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        for page in range(1, self.MAX_PAGES + 1):
            url = self.SEARCH_URL.format(base=self.BASE_URL, page=page)
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_timeout", 3000),
                    ]
                },
                callback=self.parse,
                errback=self.errback,
                dont_filter=True,
            )

    def parse(self, response):
        try:
            # Extract job cards from rendered HTML
            job_cards = response.css('div[class*="JobCard"] a[href*="/opportunities/jobs/"]')

            if not job_cards:
                # Try alternative selectors
                job_cards = response.css('a[href*="/opportunities/jobs/"]')

            logger.info(f"Glints: Found {len(job_cards)} job cards on page")

            seen_urls = set()
            for card in job_cards:
                href = card.css('::attr(href)').get()
                if not href or '/explore' in href or href in seen_urls:
                    continue
                seen_urls.add(href)

                item = self.parse_card(card, href)
                if item:
                    yield item

        except Exception as e:
            logger.error(f"Glints parse error: {e}", exc_info=True)

    def parse_card(self, card, href) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            # Extract text from card
            all_text = card.css('::text').getall()
            all_text = [t.strip() for t in all_text if t.strip()]

            job_title = all_text[0] if all_text else 'N/A'
            company = all_text[1] if len(all_text) > 1 else 'N/A'
            location = all_text[2] if len(all_text) > 2 else 'N/A'

            # Try to extract salary/type from remaining text
            remaining_text = ' '.join(all_text[3:]) if len(all_text) > 3 else ''

            salary = '0'
            salary_match = re.search(r'IDR\s*([\d,.]+)', remaining_text)
            if salary_match:
                salary = salary_match.group(1).replace('.', '').replace(',', '')

            job_type = 'N/A'
            for t in all_text:
                if any(kw in t.lower() for kw in ['full time', 'part time', 'contract', 'intern', 'freelance']):
                    job_type = t
                    break

            job_url = f"{self.BASE_URL}{href}" if not href.startswith('http') else href
            desc = f"{job_title} {company} {location} {remaining_text}"

            return {
                'job_title': self.sanitize(job_title),
                'job_location': self.sanitize(location),
                'job_department': 'N/A',
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': salary,
                'job_type': job_type,
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': self.sanitize(company),
                'job_board': 'Glints',
                'job_board_url': self.BASE_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'On-site',
                'desc': desc
            }
        except Exception as e:
            logger.error(f"Glints parse_card error: {e}")
            return None

    @staticmethod
    def sanitize(s):
        if not s: return 'N/A'
        return s.replace(',', ' -').strip()

    def errback(self, failure):
        logger.error(f"Glints request failed: {failure}")