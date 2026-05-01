import scrapy
import json
from datetime import datetime

class CourseraSpider(scrapy.Spider):
    name = "coursera"

    def start_requests(self):
        yield scrapy.Request(
            "https://www.coursera.org/search?query=data",
            callback=self.parse
        )

    def parse(self, response):
        import re
        script_text = response.text
        match = re.search(r'window\.__APOLLO_STATE__\s*=\s*(\{.*?\});\s*(?:window\.|</script>)', script_text, re.DOTALL)

        if not match:
            self.logger.error("Apollo state not found")
            return

        try:
            json_text = match.group(1)
            data = json.loads(json_text)

            for key, val in data.items():
                if isinstance(val, dict) and val.get("__typename") == "Search_ProductHit":
                    skills = val.get("skills", [])
                    desc = ", ".join(skills) if skills else val.get("tagline", "")
                    url = val.get("url", "")
                    if url and not url.startswith("http"):
                        url = f"https://www.coursera.org{url}"
                    yield {
                        "course_title": val.get("name"),
                        "desc": desc,
                        "platform": "Coursera",
                        "url": url,
                        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

        except Exception as e:
            self.logger.error(f"Error parsing Coursera JSON: {e}")
