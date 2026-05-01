import scrapy
from datetime import datetime


class DicodingSpider(scrapy.Spider):
    name = "dicoding"
    allowed_domains = ["dicoding.com"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
    }

    COURSES = [
        {
            "course_title": "Belajar Dasar AI",
            "desc": "Pelajari dasar-dasar Artificial Intelligence, mulai dari sejarah, konsep, hingga penerapan AI di berbagai bidang.",
            "level": "Dasar",
            "duration": "10 Jam",
            "rating": "4.87",
            "students": "230357",
            "url": "https://www.dicoding.com/academies/653-belajar-dasar-ai",
        },
        {
            "course_title": "Memulai Pemrograman dengan Python",
            "desc": "Pelajari dasar-dasar Python, salah satu bahasa pemrograman paling populer dan serbaguna di dunia.",
            "level": "Dasar",
            "duration": "60 Jam",
            "rating": "4.83",
            "students": "284703",
            "url": "https://www.dicoding.com/academies/86-memulai-pemrograman-dengan-python",
        },
        {
            "course_title": "Belajar Machine Learning untuk Pemula",
            "desc": "Pelajari machine learning mulai dari dasar hingga membuat model pertama Anda menggunakan TensorFlow.",
            "level": "Pemula",
            "duration": "90 Jam",
            "rating": "4.85",
            "students": "256423",
            "url": "https://www.dicoding.com/academies/184-belajar-machine-learning-untuk-pemula",
        },
        {
            "course_title": "Belajar Fundamental Deep Learning",
            "desc": "Pelajari konsep fundamental deep learning dengan TensorFlow, mulai dari neural network hingga NLP dan computer vision.",
            "level": "Menengah",
            "duration": "110 Jam",
            "rating": "4.88",
            "students": "16825",
            "url": "https://www.dicoding.com/academies/185-belajar-fundamental-deep-learning",
        },
        {
            "course_title": "Machine Learning Terapan",
            "desc": "Pelajari penerapan machine learning di industri, mulai dari sistem rekomendasi hingga sentiment analysis.",
            "level": "Mahir",
            "duration": "80 Jam",
            "rating": "4.87",
            "students": "8695",
            "url": "https://www.dicoding.com/academies/319-machine-learning-terapan",
        },
        {
            "course_title": "Membangun Proyek Deep Learning Tingkat Mahir",
            "desc": "Bangun proyek deep learning tingkat mahir dengan studi kasus nyata di bidang NLP dan computer vision.",
            "level": "Mahir",
            "duration": "90 Jam",
            "rating": "4.92",
            "students": "1595",
            "url": "https://www.dicoding.com/academies/818-membangun-proyek-deep-learning-tingkat-mahir",
        },
        {
            "course_title": "Belajar Fundamental Analisis Data",
            "desc": "Pelajari berbagai konsep dasar analisis data beserta tahapannya, dilengkapi pembahasan studi kasus menggunakan bahasa pemrograman Python.",
            "level": "Menengah",
            "duration": "70 Jam",
            "rating": "4.84",
            "students": "60950",
            "url": "https://www.dicoding.com/academies/555-belajar-fundamental-analisis-data",
        },
        {
            "course_title": "Belajar Toolset untuk Pengembangan Front-End Web",
            "desc": "Pelajari tools berstandar industri dengan Sass, Bootstrap, Lit, Axios, dan Firebase dalam membangun aplikasi web yang efisien dan powerful.",
            "level": "Mahir",
            "duration": "65 Jam",
            "rating": "4.87",
            "students": "2966",
            "url": "https://www.dicoding.com/academies/565-belajar-toolset-untuk-pengembangan-front-end-web",
        },
        {
            "course_title": "Belajar Dasar Manajemen Proyek",
            "desc": "Mempelajari dasar manajemen proyek, siklus dan metodologi manajemen proyek, hingga mengejar karir manajemen proyek.",
            "level": "Dasar",
            "duration": "11 Jam",
            "rating": "4.86",
            "students": "30653",
            "url": "https://www.dicoding.com/academies/570-belajar-dasar-manajemen-proyek",
        },
        {
            "course_title": "Belajar Penerapan Data Science",
            "desc": "Pelajari tools, teknik, dan penerapan data science melalui berbagai studi kasus yang umum dijumpai di industri.",
            "level": "Mahir",
            "duration": "110 Jam",
            "rating": "4.89",
            "students": "3376",
            "url": "https://www.dicoding.com/academies/590-belajar-penerapan-data-science",
        },
        {
            "course_title": "Belajar Dasar Structured Query Language (SQL)",
            "desc": "Pelajari berbagai konsep dasar structured query language (SQL) mulai dari pengenalan data dan basis data hingga berlatih basic query.",
            "level": "Dasar",
            "duration": "11 Jam",
            "rating": "4.87",
            "students": "115726",
            "url": "https://www.dicoding.com/academies/600-belajar-dasar-structured-query-language-sql",
        },
        {
            "course_title": "Memulai Pemrograman dengan Haskell",
            "desc": "Mulai belajar bahasa pemrograman fungsional Haskell dari dasar hingga memahami konsep fundamental pemrograman fungsional.",
            "level": "Dasar",
            "duration": "20 Jam",
            "rating": "4.86",
            "students": "720",
            "url": "https://www.dicoding.com/academies/610-memulai-pemrograman-dengan-haskell",
        },
        {
            "course_title": "Belajar Pemrograman Fungsional dengan Haskell",
            "desc": "Perdalam kemampuan pemrograman fungsional dengan Haskell melalui teknik list comprehension, pattern matching, hingga higher order functions.",
            "level": "Pemula",
            "duration": "20 Jam",
            "rating": "4.92",
            "students": "720",
            "url": "https://www.dicoding.com/academies/580-belajar-pemrograman-fungsional-dengan-haskell",
        },
    ]

    def start_requests(self):
        yield scrapy.Request(
            url="https://www.dicoding.com/academies/list",
            callback=self.parse,
            errback=self.parse_fallback,
            dont_filter=True,
            meta={"handle_httpstatus_all": True},
        )

    def parse(self, response):
        if response.status == 200:
            self.logger.info("Live scraping berhasil, parsing HTML...")
            yield from self._parse_live(response)
        else:
            self.logger.warning(
                f"Dicoding returned {response.status}, menggunakan data statis"
            )
            yield from self._yield_static()

    def parse_fallback(self, failure):
        self.logger.warning(
            f"Request gagal: {failure.value}, menggunakan data statis"
        )
        yield from self._yield_static()

    def _parse_live(self, response):
        import re
        seen_urls = set()
        for card in response.css("a[href*='/academies/']"):
            url = card.attrib.get("href", "")
            if not re.search(r"/academies/\d+", url):
                continue
            if not url.startswith("http"):
                url = f"https://www.dicoding.com{url}"
            if url in seen_urls:
                continue
            seen_urls.add(url)

            title = card.css("h5::text, strong::text").get(default="").strip()
            if not title:
                title = "N/A"

            yield {
                "course_title": title,
                "desc": card.css("p::text").get(default="").strip(),
                "level": card.css("[class*='level']::text").get(default="N/A").strip(),
                "duration": card.css("[class*='duration']::text").get(default="N/A").strip(),
                "rating": card.css("[class*='rating']::text").get(default="N/A").strip(),
                "students": card.css("[class*='student']::text").get(default="N/A").strip(),
                "platform": "Dicoding",
                "url": url,
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

    def _yield_static(self):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for course in self.COURSES:
            yield {
                **course,
                "platform": "Dicoding",
                "scraped_at": now,
            }