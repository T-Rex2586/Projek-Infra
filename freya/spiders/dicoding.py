import scrapy
from datetime import datetime
from scrapy.spiders import SitemapSpider

class DicodingSpider(SitemapSpider):
    name = "dicoding"
    allowed_domains = ["dicoding.com"]
    sitemap_urls = ['https://www.dicoding.com/academies-sitemap.xml']
    sitemap_rules = [
        ('/academies/', 'parse_detail'),
    ]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 1,
    }

    def parse_detail(self, response):
        title = response.css('title::text').get(default='N/A').replace(' - Dicoding Indonesia', '').strip()
        desc = response.css('meta[name="description"]::attr(content)').get(default='').strip()

        yield {
            "course_title": title,
            "desc": desc,
            "level": "N/A",
            "platform": "Dicoding",
            "url": response.url,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }