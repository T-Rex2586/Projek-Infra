import urllib.request, re, json

# ========================
# Test Greenhouse slugs
# ========================
print("=== GREENHOUSE ===")
gh_slugs = ['flip', 'kredivo', 'kredivocorp', 'evermos', 'blibli', 'goto', 'goto-group', 'tiket', 'tiket-com', 'gojek', 'tokopedia']
for s in gh_slugs:
    try:
        url = f'https://boards-api.greenhouse.io/v1/boards/{s}/jobs?content=true'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as r:
            d = json.loads(r.read())
            count = len(d.get('jobs', []))
            print(f'  GH {s}: {count} jobs')
    except Exception as e:
        code = getattr(e, 'code', str(e)[:30])
        print(f'  GH {s}: {code}')

# ========================
# Detect Evermos ATS
# ========================
print("\n=== EVERMOS CAREER PAGE ===")
try:
    req = urllib.request.Request('https://evermos.com/home/karir/', headers={'User-Agent': 'Mozilla/5.0 Chrome/124'})
    with urllib.request.urlopen(req, timeout=10) as r:
        body = r.read(15000).decode('utf-8', errors='ignore')
        links = re.findall(r'https?://[^\s"<>]*(greenhouse|lever|workable|kalibrr|glints|recruitee|smartrecruit)[^\s"<>]*', body, re.I)
        print(f'  ATS links: {links[:5]}')
        # Try to find job data
        job_hits = re.findall(r'"title"\s*:\s*"([^"]{5,50})"', body)[:5]
        print(f'  Job titles found: {job_hits}')
        if 'karir.evermos' in body:
            print('  Redirect to: karir.evermos')
        if 'careers.evermos' in body:
            print('  Redirect to: careers.evermos')
except Exception as e:
    print(f'  ERROR: {e}')

# ========================
# Check GoTo career
# ========================
print("\n=== GoTo CAREER OPTIONS ===")
goto_urls = [
    'https://www.gotogroup.com/careers',
    'https://careers.goto.com',
    'https://api.lever.co/v0/postings/gotogroup?mode=json',
    'https://api.lever.co/v0/postings/getgo?mode=json',
]
for url in goto_urls:
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 Chrome/124'})
        with urllib.request.urlopen(req, timeout=5) as r:
            body = r.read(1000).decode('utf-8', errors='ignore')
            d = None
            try:
                d = json.loads(body)
            except:
                pass
            count = len(d) if isinstance(d, list) else '?'
            print(f'  {url}: {r.status} ({count})')
    except Exception as e:
        code = getattr(e, 'code', str(e)[:40])
        print(f'  {url}: {code}')
