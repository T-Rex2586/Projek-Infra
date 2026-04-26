import json

def parse_next_data(html_text):
    import re
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">({.*?})</script>', html_text, re.DOTALL)
    return bool(match)

if __name__ == "__main__":
    pass
