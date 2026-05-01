import scrapy
import json
import logging
from datetime import datetime
from typing import Optional, AsyncGenerator
from scrapy_playwright.page import PageMethod
from freya.pipelines import calculate_job_age
from freya.utils import clean_string

logger = logging.getLogger(__name__)

class JobstreetSpider(scrapy.Spider):
    """
    Jobstreet Indonesia spider menggunakan Playwright headless browser.
    Chalice Search API diproteksi Cloudflare dari server-side, sehingga
    perlu browser nyata untuk mendapatkan session cookie yang valid.

    Flow:
    1. Buka halaman search Jobstreet via Playwright (dapat cookies)
    2. Intercept response dari Chalice API yang diload oleh browser
    3. Parse JSON jobs dari response

    Fallback: Parse HTML jobs card langsung jika API intercept gagal
    """
    name = 'jobstreet'
    BASE_URL = 'https://id.jobstreet.com'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2.0,
        'DOWNLOAD_TIMEOUT': 60,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'PLAYWRIGHT_BROWSER_TYPE': 'chromium',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'args': [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-software-rasterizer',
                '--disable-extensions',
            ],
        },
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 45000,
        'AUTOTHROTTLE_ENABLED': False,
    }

    SEARCH_QUERIES = [
        'data engineer',
        'data analyst',
        'software engineer',
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.seen_urls = set()

    async def start(self) -> AsyncGenerator:
        """
        Scrapy 2.13+ compliant async generator.
        Mulai request untuk setiap keyword search.
        """
        for query in self.SEARCH_QUERIES:
            search_url = f"{self.BASE_URL}/id/jobs?keywords={query.replace(' ', '+')}&location=Indonesia"
            yield scrapy.Request(
                search_url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_timeout", 3000),
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                    ],
                    "playwright_page_goto_kwargs": {
                        "wait_until": "domcontentloaded",
                        "timeout": 45000,
                    },
                },
                callback=self.parse,
                errback=self.errback,
                cb_kwargs={"query": query},
                dont_filter=True,
            )

    async def parse(self, response, query=''):
        """Parse Jobstreet search results page."""
        page = response.meta.get("playwright_page")

        try:

            scripts = response.css('script[type="application/ld+json"]::text').getall()
            jobs_found = 0

            for script in scripts:
                try:
                    data = json.loads(script)
                    if isinstance(data, list):
                        for item in data:
                            if item.get('@type') == 'JobPosting':
                                parsed = self.parse_ld_json(item)
                                if parsed and parsed['job_url'] not in self.seen_urls:
                                    self.seen_urls.add(parsed['job_url'])
                                    yield parsed
                                    jobs_found += 1
                    elif data.get('@type') == 'JobPosting':
                        parsed = self.parse_ld_json(data)
                        if parsed and parsed['job_url'] not in self.seen_urls:
                            self.seen_urls.add(parsed['job_url'])
                            yield parsed
                            jobs_found += 1
                except json.JSONDecodeError:
                    continue

            if jobs_found > 0:
                logger.info(f"Jobstreet LD+JSON '{query}': {jobs_found} jobs")
            else:

                jobs_found = 0

                next_data_script = response.css('script[id="__NEXT_DATA__"]::text').get()
                if next_data_script:
                    try:
                        nd = json.loads(next_data_script)
                        jobs_from_next = self._extract_from_next_data(nd)
                        for job in jobs_from_next:
                            if job['job_url'] not in self.seen_urls:
                                self.seen_urls.add(job['job_url'])
                                yield job
                                jobs_found += 1
                        logger.info(f"Jobstreet __NEXT_DATA__ '{query}': {jobs_found} jobs")
                    except Exception as e:
                        logger.warning(f"Jobstreet __NEXT_DATA__ parse error: {e}")

                if jobs_found == 0:

                    for item in self._parse_html_cards(response, query):
                        if item['job_url'] not in self.seen_urls:
                            self.seen_urls.add(item['job_url'])
                            yield item
                            jobs_found += 1

        except Exception as e:
            logger.error(f"Jobstreet parse error '{query}': {e}", exc_info=True)
        finally:
            if page:
                await page.close()

    def _extract_from_next_data(self, data: dict) -> list:
        """Extract jobs from Next.js __NEXT_DATA__ script."""
        results = []
        try:

            props = (
                data.get('props', {})
                    .get('pageProps', {})
            )
            jobs = (
                props.get('initialCandidateExperimentalData', {}).get('jobs')
                or props.get('jobs')
                or props.get('searchResults', {}).get('jobs')
                or []
            )

            for job in jobs:
                if not isinstance(job, dict):
                    continue
                url = job.get('jobUrl') or job.get('url', '')
                if url and not url.startswith('http'):
                    url = self.BASE_URL + url
                if not url:
                    continue

                title = clean_string(job.get('title') or job.get('jobTitle') or 'N/A')
                company = clean_string(
                    job.get('advertiser', {}).get('description', 'Private Advertiser')
                    if isinstance(job.get('advertiser'), dict)
                    else 'Private Advertiser'
                )
                loc = job.get('location') or job.get('locationDescription') or 'Indonesia'
                if isinstance(loc, dict):
                    loc = loc.get('label') or loc.get('name', 'Indonesia')

                dept = 'N/A'
                cats = job.get('classification') or job.get('categories')
                if isinstance(cats, dict):
                    dept = cats.get('description', 'N/A')
                elif isinstance(cats, list) and cats:
                    dept = cats[0].get('name', 'N/A') if isinstance(cats[0], dict) else 'N/A'

                posted = job.get('listingDate') or job.get('postedAt') or ''
                last_seen = self.format_date(posted) if posted else self.timestamp

                results.append({
                    'job_title': title,
                    'job_location': clean_string(str(loc)),
                    'job_department': clean_string(dept),
                    'job_url': url,
                    'first_seen': self.timestamp,
                    'base_salary': '0',
                    'job_type': 'Full-time',
                    'job_level': 'N/A',
                    'job_apply_end_date': self.format_date(job.get('expiryDate', '')),
                    'last_seen': last_seen,
                    'is_active': 'True',
                    'company': company,
                    'job_board': 'Jobstreet',
                    'job_board_url': self.BASE_URL,
                    'job_age': calculate_job_age(self.timestamp, last_seen),
                    'work_arrangement': 'On-site',
                    'desc': f"{title} {dept}",
                })
        except Exception as e:
            logger.error(f"Jobstreet _extract_from_next_data error: {e}")
        return results

    def _parse_html_cards(self, response, query):
        """Parse HTML job cards - selector confirmed via debug."""

        cards = response.css('article[data-testid="job-card"]')
        if not cards:
            cards = response.css('article')

        logger.info(f"Jobstreet HTML: found {len(cards)} cards for '{query}'")
        count = 0

        for card in cards:
            try:

                title_link = card.css('a[href*="/id/job/"][href*="cardTitle"]')
                if not title_link:
                    title_link = card.css('a[href*="/id/job/"]')
                if not title_link:
                    continue

                href = title_link.attrib.get('href', '')
                title_text = title_link.css('::text').get('').strip()

                if not title_text:
                    title_text = (
                        card.css('h3::text, h2::text, [data-automation="jobTitle"]::text').get()
                        or 'N/A'
                    )

                job_url = href if href.startswith('http') else (self.BASE_URL + href if href else '')
                if '?' in job_url:
                    job_url = job_url.split('?')[0]

                if not job_url:
                    continue

                company = clean_string(
                    card.css('[data-automation="jobCompany"]::text').get()
                    or card.css('span[class*="company"]::text').get()
                    or 'Private Advertiser'
                )
                location = clean_string(
                    card.css('[data-automation="jobLocation"]::text').get()
                    or card.css('span[class*="location"]::text').get()
                    or 'Indonesia'
                )
                salary_text = card.css('[data-automation="jobSalary"]::text').get() or '0'

                item = {
                    'job_title': clean_string(title_text),
                    'job_location': location,
                    'job_department': 'N/A',
                    'job_url': job_url,
                    'first_seen': self.timestamp,
                    'base_salary': salary_text,
                    'job_type': 'Full-time',
                    'job_level': 'N/A',
                    'job_apply_end_date': self.timestamp,
                    'last_seen': self.timestamp,
                    'is_active': 'True',
                    'company': company,
                    'job_board': 'Jobstreet',
                    'job_board_url': self.BASE_URL,
                    'job_age': '0',
                    'work_arrangement': 'On-site',
                    'desc': f"{clean_string(title_text)} {company}",
                }
                yield item
                count += 1

            except Exception as e:
                logger.error(f"Jobstreet card parse error: {e}")

        logger.info(f"Jobstreet HTML cards '{query}': yielded {count} jobs")

    def parse_ld_json(self, job: dict) -> Optional[dict]:
        """Parse JobPosting LD+JSON structured data."""
        try:
            url = job.get('url') or ''
            if not url:
                return None

            title = clean_string(job.get('title', 'N/A'))
            company = clean_string(
                job.get('hiringOrganization', {}).get('name', 'Private Advertiser')
                if isinstance(job.get('hiringOrganization'), dict)
                else 'Private Advertiser'
            )
            loc = job.get('jobLocation') or {}
            location = 'Indonesia'
            if isinstance(loc, dict):
                addr = loc.get('address') or {}
                if isinstance(addr, dict):
                    location = addr.get('addressLocality') or addr.get('addressRegion') or 'Indonesia'
            elif isinstance(loc, list) and loc:
                addr = loc[0].get('address', {}) if isinstance(loc[0], dict) else {}
                location = addr.get('addressLocality', 'Indonesia')

            date_posted = job.get('datePosted', '')
            last_seen = self.format_date(date_posted) if date_posted else self.timestamp

            return {
                'job_title': title,
                'job_location': clean_string(location),
                'job_department': 'N/A',
                'job_url': url,
                'first_seen': self.timestamp,
                'base_salary': '0',
                'job_type': job.get('employmentType', 'Full-time'),
                'job_level': 'N/A',
                'job_apply_end_date': self.format_date(job.get('validThrough', '')),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': company,
                'job_board': 'Jobstreet',
                'job_board_url': self.BASE_URL,
                'job_age': calculate_job_age(self.timestamp, last_seen),
                'work_arrangement': 'On-site',
                'desc': clean_string(str(job.get('description', ''))[:300]),
            }
        except Exception as e:
            logger.error(f"Jobstreet parse_ld_json error: {e}")
            return None

    def format_date(self, date_string) -> str:
        if not date_string:
            return self.timestamp
        try:
            dt = datetime.fromisoformat(str(date_string).replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return self.timestamp

    def errback(self, failure):
        logger.error(f"Jobstreet request failed: {failure.getErrorMessage()}")
