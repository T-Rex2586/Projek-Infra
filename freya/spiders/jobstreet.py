import scrapy
import json
from datetime import datetime, timedelta
import random
import logging
from typing import Dict, Any, Optional
from freya.pipelines import calculate_job_age
from freya.utils import clean_string

logger = logging.getLogger(__name__)

class JobstreetSpider(scrapy.Spider):
    name = 'jobstreet'
    # 🔥 Endpoint baru: SEEK GraphQL (Jobstreet sudah migrasi ke SEEK platform 2026)
    GRAPHQL_URL = 'https://xapi.supercharge-srp.co/job-search/graphql?country=id&is498SmartSortABEnabled=false'
    MAX_PAGES = 20
    JOBS_PER_PAGE = 30

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2.0,
        # Disable playwright for API calls
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        },
    }

    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_requests(self):
        for page in range(1, self.MAX_PAGES + 1):
            yield self.create_request(page)

    def create_request(self, page):
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-ID,en;q=0.9,id;q=0.8',
            'content-type': 'application/json',
            'origin': 'https://id.jobstreet.com',
            'referer': 'https://id.jobstreet.com/',
            'user-agent': random.choice(self.USER_AGENTS),
        }

        payload = {
            "query": "query getJobs($country: String, $locale: String, $keyword: String, $createdAt: String, $jobFunctions: [Int], $categories: [String], $locations: [Int], $careerLevels: [Int], $minSalary: Int, $maxSalary: Int, $salaryType: Int, $candidateSalary: Int, $candidateSalaryCurrency: String, $dateRange: Int, $jobType: String, $workArrangement: String, $nearme: NearMeInput, $distanceKm: Int, $sort: String, $sVi: String, $solVisitorId: String, $page: Int, $pageSize: Int, $companyId: String, $advertiserId: String, $userAgent: String, $accNums: Int, $subAccount: Int, $minEdu: Int, $maxEdu: Int, $edoIds: [Int], $fields: String) {\n  jobs(\n    country: $country\n    locale: $locale\n    keyword: $keyword\n    createdAt: $createdAt\n    jobFunctions: $jobFunctions\n    categories: $categories\n    locations: $locations\n    careerLevels: $careerLevels\n    minSalary: $minSalary\n    maxSalary: $maxSalary\n    salaryType: $salaryType\n    candidateSalary: $candidateSalary\n    candidateSalaryCurrency: $candidateSalaryCurrency\n    dateRange: $dateRange\n    jobType: $jobType\n    workArrangement: $workArrangement\n    nearme: $nearme\n    distanceKm: $distanceKm\n    sort: $sort\n    sVi: $sVi\n    solVisitorId: $solVisitorId\n    page: $page\n    pageSize: $pageSize\n    companyId: $companyId\n    advertiserId: $advertiserId\n    userAgent: $userAgent\n    accNums: $accNums\n    subAccount: $subAccount\n    minEdu: $minEdu\n    maxEdu: $maxEdu\n    edoIds: $edoIds\n    fields: $fields\n  ) {\n    total\n    jobs {\n      id\n      adType\n      sourceCountryCode\n      isStandout\n      companyMeta {\n        id\n        advertiserId\n        isPrivate\n        name\n        logoUrl\n        slug\n      }\n      jobTitle\n      jobUrl\n      jobTitleSlug\n      description\n      employmentTypes {\n        code\n        name\n      }\n      categories {\n        code\n        name\n      }\n      locations {\n        code\n        name\n        slug\n        children {\n          code\n          name\n          slug\n        }\n      }\n      salary {\n        max\n        min\n        type\n        currency\n      }\n      isQuickApply\n      workArrangements {\n        code\n      }\n      postedAt\n      closingDate\n      isClassified\n      solMetadata\n    }\n  }\n}",
            "variables": {
                "keyword": "data",
                "jobFunctions": [],
                "locations": [],
                "salaryType": 1,
                "jobType": "",
                "createdAt": "",
                "careerLevels": [],
                "page": page,
                "pageSize": self.JOBS_PER_PAGE,
                "country": "id",
                "sort": "relevance",
                "locale": "id",
                "sVi": "",
                "solVisitorId": ""
            }
        }

        return scrapy.Request(
            self.GRAPHQL_URL,
            method='POST',
            headers=headers,
            body=json.dumps(payload),
            callback=self.parse,
            meta={'page': page},
            errback=self.errback_httpbin,
            dont_filter=True,
        )

    def parse(self, response):
        try:
            raw_body = response.body.decode('utf-8', errors='replace')
            data = json.loads(raw_body)

            jobs_data = data.get('data', {}).get('jobs', {})
            if not jobs_data:
                logger.warning(f"Jobstreet: No data on page {response.meta['page']}. Keys: {list(data.keys())}")
                # Check for errors
                if 'errors' in data:
                    logger.error(f"Jobstreet GraphQL errors: {data['errors']}")
                return

            total = jobs_data.get('total', 0)
            jobs = jobs_data.get('jobs', [])

            if not jobs:
                logger.info(f"Jobstreet: No more jobs on page {response.meta['page']}")
                return

            logger.info(f"Jobstreet: Page {response.meta['page']}, got {len(jobs)} jobs (total: {total})")

            for job in jobs:
                item = self.parse_job(job)
                if item:
                    yield item

        except json.JSONDecodeError as e:
            logger.error(f"Jobstreet JSON decode error: {e}")
        except Exception as e:
            logger.error(f"Error parsing Jobstreet: {e}", exc_info=True)

    def parse_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp

            posted_at = job.get('postedAt', '')
            last_seen = self.format_date(posted_at) if posted_at else first_seen

            job_title = clean_string(job.get('jobTitle', 'N/A'))

            # Categories
            categories = job.get('categories', [])
            department = categories[0].get('name', 'N/A') if categories and isinstance(categories[0], dict) else 'N/A'

            # Locations
            locations = job.get('locations', [])
            location_str = 'Indonesia'
            if locations:
                loc = locations[0]
                if isinstance(loc, dict):
                    location_str = loc.get('name', 'Indonesia')

            # Company
            company_meta = job.get('companyMeta', {})
            company_name = 'Private Advertiser'
            if isinstance(company_meta, dict):
                if not company_meta.get('isPrivate'):
                    company_name = company_meta.get('name', 'Private Advertiser')

            # Salary
            salary_obj = job.get('salary', {})
            base_salary = '0'
            if isinstance(salary_obj, dict) and salary_obj.get('min'):
                base_salary = str(salary_obj['min'])

            # Employment type
            emp_types = job.get('employmentTypes', [])
            job_type = emp_types[0].get('name', 'N/A') if emp_types and isinstance(emp_types[0], dict) else 'N/A'

            # Work arrangement
            work_arr = job.get('workArrangements', [])
            work_arrangement = 'On-site'
            if work_arr:
                wa = work_arr[0]
                if isinstance(wa, dict):
                    code = wa.get('code', '')
                    if code == 'remote': work_arrangement = 'Remote'
                    elif code == 'hybrid': work_arrangement = 'Hybrid'

            # Closing date
            closing_date = job.get('closingDate', '')
            apply_end = self.format_date(closing_date) if closing_date else ''

            # Description for skill extraction
            desc_text = job.get('description', '')
            full_desc = f"{job_title} {department} {clean_string(desc_text)}"

            job_url = job.get('jobUrl', '')
            if not job_url:
                job_url = f"https://id.jobstreet.com/job/{job.get('id', '')}"

            return {
                'job_title': job_title,
                'job_location': clean_string(location_str),
                'job_department': clean_string(department),
                'job_url': job_url,
                'first_seen': first_seen,
                'base_salary': base_salary,
                'job_type': clean_string(job_type),
                'job_level': 'N/A',
                'job_apply_end_date': apply_end,
                'last_seen': last_seen,
                'is_active': 'True',
                'company': clean_string(company_name),
                'job_board': 'Jobstreet',
                'job_board_url': 'https://id.jobstreet.com/',
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': work_arrangement,
                'desc': full_desc
            }
        except Exception as e:
            logger.error(f"Jobstreet parse_job error: {e}")
            return None

    def format_date(self, date_string):
        if not date_string: return self.timestamp
        try:
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return self.timestamp

    def errback_httpbin(self, failure):
        logger.error(f"Jobstreet HTTP Error: {failure}")