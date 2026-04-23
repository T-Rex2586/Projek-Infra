import scrapy
from datetime import datetime
import logging
from typing import Dict, Any, Optional
from scrapy_playwright.page import PageMethod
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class LokeridSpider(scrapy.Spider):
    name = 'lokerid'
    BASE_URL = 'https://www.loker.id'
    SEARCH_URL = 'https://www.loker.id/cari-lowongan-kerja/page/{page}?q=IT'
    
    MAX_PAGES = 500

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2.0,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        for page in range(1, self.MAX_PAGES + 1):
            yield scrapy.Request(
                self.SEARCH_URL.format(page=page),
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_timeout", 5000)
                    ]
                },
                callback=self.parse,
                errback=self.errback
            )

    def parse(self, response):
        try:
            # Menggunakan semua link yang menuju ke loker.id/lowongan-kerja/
            job_cards = response.css('a[href*="/lowongan-kerja/"]')
            
            if not job_cards:
                job_cards = response.css('div.media')

            logger.info(f"LokerID: Found {len(job_cards)} job links on page")

            seen_urls = set()
            for card in job_cards:
                href = card.css('::attr(href)').get()
                if not href or href in seen_urls or '/page/' in href:
                    continue
                    
                seen_urls.add(href)
                item = self.parse_card(card, href)
                if item:
                    yield item

        except Exception as e:
            logger.error(f"LokerID parse error: {e}", exc_info=True)

    def parse_card(self, card, href) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            all_text = card.css('::text').getall()
            all_text = [t.strip() for t in all_text if t.strip()]

            job_title = all_text[0] if all_text else 'N/A'
            company = all_text[1] if len(all_text) > 1 else 'N/A'
            location = 'N/A'
            
            for t in all_text:
                if 'Kota' in t or 'Kabupaten' in t or 'Jakarta' in t:
                    location = t
                    break

            job_url = href

            return {
                'job_title': self.sanitize(job_title),
                'job_location': self.sanitize(location),
                'job_department': 'IT',
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': 'N/A',
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': self.sanitize(company),
                'job_board': 'Loker.id',
                'job_board_url': self.BASE_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'N/A',
                'desc': f"{job_title} at {company}"
            }
        except Exception as e:
            logger.error(f"LokerID parse_card error: {e}")
            return None

    @staticmethod
    def sanitize(s):
        if not s: return 'N/A'
        return s.replace('\n', ' ').strip()

    def errback(self, failure):
        logger.error(f"LokerID request failed: {failure}")
