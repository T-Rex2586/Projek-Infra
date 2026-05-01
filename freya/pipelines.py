from itemadapter import ItemAdapter
from datetime import datetime
from scrapy.exceptions import DropItem
import logging

logger = logging.getLogger(__name__)

def calculate_job_age(first_seen, last_seen):
    try:
        if not first_seen or not last_seen:
            return 'unknown'
        date_format = "%Y-%m-%d %H:%M:%S"
        first_seen_date = datetime.strptime(str(first_seen), date_format)
        last_seen_date = datetime.strptime(str(last_seen), date_format)
        diff_days = abs(last_seen_date - first_seen_date).days
        if diff_days <= 1:    return 'new'
        elif diff_days <= 7:  return 'hot'
        elif diff_days <= 15: return 'recent'
        elif diff_days <= 21: return 'aging'
        elif diff_days <= 30: return 'old'
        else:                 return 'expired'
    except (ValueError, TypeError):
        return 'unknown'

class FreyaPipeline:
    """
    Pipeline utama Freya.
     Fix Scrapy 2.15 DeprecationWarning:
       - open_spider / close_spider / process_item TIDAK boleh punya 'spider' param
         jika tidak digunakan dari from_crawler
    """

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls()
        instance.stats = crawler.stats
        return instance

    def open_spider(self):
        logger.info("FreyaPipeline: Spider opened")

    def close_spider(self):
        logger.info("FreyaPipeline: Spider closed")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        first_seen = adapter.get('first_seen', '')
        last_seen  = adapter.get('last_seen', '')
        adapter['job_age'] = calculate_job_age(first_seen, last_seen) if (first_seen and last_seen) else 'unknown'

        if adapter.get('platform'):
            if not adapter.get('url'):
                raise DropItem(f"[{spider.name}] Missing url: title={adapter.get('course_title')}")
        else:
            if not adapter.get('job_url'):
                raise DropItem(f"[{spider.name}] Missing job_url: title={adapter.get('job_title')}")

        return item
