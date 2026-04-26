from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    page.goto('https://www.dicoding.com/academies', wait_until='networkidle')
    html = page.content()
    with open('/opt/airflow/dags/dicoding_dump.html', 'w', encoding='utf-8') as f:
        f.write(html)
    browser.close()
