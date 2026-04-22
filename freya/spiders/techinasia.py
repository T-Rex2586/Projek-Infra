import scrapy
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional
from urllib.parse import urlencode
from freya.pipelines import calculate_job_age
from freya.utils import calculate_job_apply_end_date

logger = logging.getLogger(__name__)

class TechInAsiaSpider(scrapy.Spider):
    name = 'techinasia'
    BASE_URL = 'https://219wx3mpv4-dsn.algolia.net/1/indexes/*/queries'

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
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
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:130.0) Gecko/20100101 Firefox/130.0',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.5',
            'content-type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www.techinasia.com',
            'Referer': 'https://www.techinasia.com/',
        }

        params = {
            'x-algolia-agent': 'Algolia for vanilla JavaScript 3.30.0;JS Helper 2.26.1',
            'x-algolia-application-id': '219WX3MPV4',
            'x-algolia-api-key': 'b528008a75dc1c4402bfe0d8db8b3f8e',
        }

        payload = {
            "requests": [
                {
                    "indexName": "job_postings",
                    "params": "query=&hitsPerPage=20&maxValuesPerFacet=1000&page=0&facets=%5B%22*%22%2C%22city.work_country_name%22%2C%22position.name%22%2C%22industries.vertical_name%22%2C%22experience%22%2C%22job_type.name%22%2C%22is_salary_visible%22%2C%22has_equity%22%2C%22currency.currency_code%22%2C%22salary_min%22%2C%22taxonomies.slug%22%5D&tagFilters=&facetFilters=%5B%5B%22city.work_country_name%3AIndonesia%22%5D%5D"
                },
                {
                    "indexName": "job_postings",
                    "params": "query=&hitsPerPage=1&maxValuesPerFacet=1000&page=0&attributesToRetrieve=%5B%5D&attributesToHighlight=%5B%5D&attributesToSnippet=%5B%5D&tagFilters=&analytics=false&clickAnalytics=false&facets=city.work_country_name"
                }
            ]
        }

        yield scrapy.Request(
            f"{self.BASE_URL}?{urlencode(params)}",
            method='POST',
            headers=headers,
            body=json.dumps(payload),
            callback=self.parse,
            dont_filter=True
        )

    def parse(self, response):
        try:
            data = json.loads(response.text)
            results = data.get('results', [])
            if not results:
                logger.error("TechInAsia: No results in response")
                return

            hits = results[0].get('hits', [])
            total_pages = results[0].get('nbPages', 0)
            current_page = results[0].get('page', 0)

            logger.info(f"TechInAsia: Page {current_page}/{total_pages}, got {len(hits)} hits")

            for job in hits:
                item = self.parse_job(job)
                if item:
                    yield item

            # Handle pagination
            if current_page < total_pages - 1:
                payload = json.loads(response.request.body)
                params_str = payload['requests'][0]['params']
                new_params_str = params_str.replace(f"page={current_page}", f"page={current_page + 1}")
                payload['requests'][0]['params'] = new_params_str

                yield scrapy.Request(
                    response.url,
                    method='POST',
                    headers=response.request.headers,
                    body=json.dumps(payload),
                    callback=self.parse,
                    dont_filter=True
                )

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)

    def parse_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            first_seen = self.timestamp
            published_at = job.get('published_at', '')
            last_seen = self.format_datetime(published_at) if published_at else first_seen

            # Build desc for skill extraction
            description = job.get('description_text', '') or job.get('description', '')
            position_name = ''
            position_obj = job.get('position')
            if isinstance(position_obj, dict):
                position_name = position_obj.get('name', '')
            
            job_title = job.get('title', 'N/A')
            full_desc = f"{job_title} {position_name} {description}"

            # Salary
            salary_min = job.get('salary_min')
            salary_max = job.get('salary_max')
            currency_obj = job.get('currency', {})
            currency = currency_obj.get('currency_code', '') if isinstance(currency_obj, dict) else ''
            
            if salary_min is not None:
                base_salary = str(salary_min)
            else:
                base_salary = '0'

            # Location
            city_obj = job.get('city', {})
            if isinstance(city_obj, dict):
                city_name = city_obj.get('name', 'N/A')
                country_name = city_obj.get('work_country_name', 'N/A')
            else:
                city_name = 'N/A'
                country_name = 'N/A'

            # Company
            company_obj = job.get('company', {})
            if isinstance(company_obj, dict):
                company_name = company_obj.get('name', 'N/A')
                company_slug = company_obj.get('entity_slug', '')
            else:
                company_name = 'N/A'
                company_slug = ''

            # Job type
            job_type_obj = job.get('job_type', {})
            job_type = job_type_obj.get('name', 'N/A') if isinstance(job_type_obj, dict) else 'N/A'

            return {
                'job_title': self.sanitize_string(job_title),
                'job_location': f"{city_name} - {country_name}",
                'job_department': self.sanitize_string(position_name),
                'job_url': f"https://www.techinasia.com/jobs/{job.get('id')}",
                'first_seen': first_seen,
                'base_salary': base_salary,
                'job_type': self.sanitize_string(job_type),
                'job_level': f"{job.get('experience_min', 'N/A')} - {job.get('experience_max', 'N/A')} years",
                'job_apply_end_date': calculate_job_apply_end_date(last_seen),
                'last_seen': last_seen,
                'is_active': 'True',
                'company': self.sanitize_string(company_name),
                'company_url': f"https://www.techinasia.com/companies/{company_slug}",
                'job_board': 'Tech in Asia Jobs',
                'job_board_url': 'https://www.techinasia.com/jobs',
                'job_age': calculate_job_age(first_seen, last_seen),
                'work_arrangement': 'Remote' if job.get('is_remote') else 'On-site',
                'desc': full_desc  # WAJIB untuk Airflow skill extraction
            }
        except Exception as e:
            logger.error(f"TechInAsia parse_job error: {e}")
            return None

    def format_datetime(self, date_string: str) -> str:
        if not date_string:
            return self.timestamp
        try:
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            try:
                dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                return self.timestamp

    @staticmethod
    def sanitize_string(s: Optional[str]) -> str:
        if s is None:
            return 'N/A'
        return s.strip().replace(',', ' -').replace('\n', ' ').replace('\r', '')
