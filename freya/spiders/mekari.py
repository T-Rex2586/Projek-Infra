import scrapy
from scrapy_playwright.page import PageMethod
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class MekariSpider(scrapy.Spider):
    name = 'mekari'
    BASE_URL = 'https://mekari.hire.trakstar.com'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        yield scrapy.Request(
            self.BASE_URL,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "div.js-card.list-item", timeout=15000),
                ]
            },
            callback=self.parse,
            errback=self.errback,
        )

    async def parse(self, response):
        page = response.meta.get("playwright_page")

        try:
            total = 0
            while True:
                for job in response.css('div.js-card.list-item'):
                    item = self.parse_job(job)
                    if item:
                        yield item
                        total += 1

                if not page:
                    break

                next_button = await page.query_selector('ul.pagination li.page-item:last-child a.page-link[href*="/?p="]')
                if next_button:
                    await next_button.click()
                    await page.wait_for_load_state("networkidle")
                    content = await page.content()
                    response = scrapy.http.HtmlResponse(url=page.url, body=content, encoding='utf-8')
                else:
                    break

            logger.info(f"Mekari: Scraped {total} jobs")

        except Exception as e:
            logger.error(f"Error parsing page: {e}", exc_info=True)
        finally:
            if page:
                await page.close()

    def parse_job(self, selector) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            last_seen = self.timestamp

            job_title = self.sanitize_string(selector.css('h3.js-job-list-opening-name::text').get())
            job_location = self.sanitize_string(selector.css('div.js-job-list-opening-loc::text').get())
            job_department = self.sanitize_string(selector.css('div.col-md-4.col-xs-12 div.rb-text-4:first-child::text').get())
            job_type = self.sanitize_string(selector.css('div.js-job-list-opening-meta span:first-child::text').get())
            work_arrangement = self.sanitize_string(selector.css('div.js-job-list-opening-meta span:last-child::text').get())

            href = selector.css('a::attr(href)').get() or ''
            job_url = self.BASE_URL + href if href else ''

            desc = f"{job_title} {job_department} {job_type}"

            return {
                'job_title': job_title,
                'job_location': job_location,
                'job_department': job_department,
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': '0',
                'job_type': job_type,
                'job_level': 'N/A',
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': 'Mekari',
                'company_url': self.BASE_URL,
                'job_board': 'Mekari',
                'job_board_url': self.BASE_URL,
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': work_arrangement,
                'desc': desc
            }
        except Exception as e:
            logger.error(f"Mekari parse_job error: {e}")
            return None

    def errback(self, failure):
        logger.error(f"Mekari request error: {failure}")

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        if s:
            return ' - '.join(part.strip() for part in s.split(',') if part.strip())
        return 'N/A'
