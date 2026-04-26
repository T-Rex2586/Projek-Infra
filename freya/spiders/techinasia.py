import scrapy
from datetime import datetime
import logging
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class TechInAsiaSpider(scrapy.Spider):
    name = 'techinasia'

    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
        }
    }

    def start_requests(self):
        yield scrapy.Request(
            url="https://www.techinasia.com/jobs",
            meta={"playwright": True},
            callback=self.parse
        )

    def parse(self, response):
        links = response.css('a::attr(href)').getall()
        job_links = set()
        for link in links:
            if '/jobs/' in link and link != '/jobs' and 'search' not in link and 'employers' not in link:
                job_links.add(response.urljoin(link))

        for url in job_links:
            yield scrapy.Request(
                url=url,
                meta={"playwright": True},
                callback=self.parse_job
            )

    def parse_job(self, response):
        title_text = response.css('title::text').get(default='').strip()
        job_title = "N/A"
        company = "N/A"
        if ' at ' in title_text and ' - Tech in Asia' in title_text:
            parts = title_text.split(' at ')
            job_title = parts[0].strip()
            company = parts[1].replace(' - Tech in Asia', '').strip()
        elif title_text:
            job_title = title_text.replace(' - Tech in Asia', '').strip()

        desc_parts = response.css('p::text, p *::text, span::text').getall()
        desc = " ".join([p.strip() for p in desc_parts if p.strip()])

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        yield {
            'job_title': job_title,
            'job_location': 'N/A',
            'job_department': 'N/A',
            'job_url': response.url,
            'first_seen': now,
            'base_salary': '0',
            'job_type': 'N/A',
            'job_level': 'N/A',
            'job_apply_end_date': calculate_job_apply_end_date(now),
            'last_seen': now,
            'is_active': 'True',
            'company': company,
            'company_url': '',
            'job_board': 'Tech in Asia Jobs',
            'job_board_url': 'https://www.techinasia.com/jobs',
            'job_age': 0,
            'work_arrangement': 'N/A',
            'desc': desc
        }
