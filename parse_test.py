import re
html = open('c:\\infrastruktur\\projek\\dags\\techinasia_job.html', encoding='utf-8').read()
ld_json = re.search(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL)
if ld_json:
    print(ld_json.group(1))
