"""
Debug script untuk melihat struktur HTML Jobstreet dan menemukan selector yang benar.
"""
from playwright.sync_api import sync_playwright
import json

with sync_playwright() as p:
    browser = p.chromium.launch(args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'])
    page = browser.new_page(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36'
    )
    
    print("Navigating to Jobstreet...")
    page.goto('https://id.jobstreet.com/id/jobs?keywords=data+engineer&location=Indonesia', 
              wait_until='networkidle', timeout=30000)
    
    # Cek HTML structure
    html = page.content()
    
    # Test selectors
    selectors = [
        'article[data-testid="job-card"]',
        'article[class*="job-card"]',
        '[data-automation="jobListing"]',
        'article',
        '[class*="JobCard"]',
        '[data-id]',
    ]
    
    for sel in selectors:
        count = len(page.query_selector_all(sel))
        print(f"Selector '{sel}': {count} elements")
    
    # Cari link patterns
    links = page.query_selector_all('a[href*="/id/job/"]')
    print(f"\nLinks with /id/job/: {len(links)}")
    if links:
        for i, link in enumerate(links[:3]):
            href = link.get_attribute('href')
            text = link.inner_text()[:50]
            print(f"  Link {i}: href={href} | text={text}")
    
    # Cek __NEXT_DATA__
    next_data = page.query_selector('script#__NEXT_DATA__')
    if next_data:
        content = next_data.inner_text()
        data = json.loads(content)
        props = data.get('props', {}).get('pageProps', {})
        print(f"\n__NEXT_DATA__ keys in pageProps: {list(props.keys())[:10]}")
        # Cari jobs
        for key in props:
            val = props[key]
            if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                if 'title' in val[0] or 'id' in val[0]:
                    print(f"  Potential jobs key: '{key}' ({len(val)} items)")
                    print(f"  Sample: {json.dumps(val[0])[:200]}")
    else:
        print("\nNo __NEXT_DATA__ found")
    
    # Cek LD+JSON
    ld_scripts = page.query_selector_all('script[type="application/ld+json"]')
    print(f"\nLD+JSON scripts: {len(ld_scripts)}")
    for i, script in enumerate(ld_scripts[:2]):
        text = script.inner_text()
        data = json.loads(text)
        dtype = data.get('@type') if isinstance(data, dict) else [d.get('@type') for d in data[:2]]
        print(f"  Script {i}: @type = {dtype}")
    
    browser.close()
