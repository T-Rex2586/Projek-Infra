from datetime import datetime, timedelta
import re

def calculate_job_apply_end_date(last_seen):
    try:
        last_seen_date = datetime.strptime(last_seen, "%Y-%m-%d %H:%M:%S")
        end_date = last_seen_date + timedelta(days=30)
        return end_date.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return 'N/A'

def clean_string(s):
    """Clean string dari karakter yang tidak diinginkan."""
    if not s:
        return 'N/A'
    if not isinstance(s, str):
        s = str(s)
    # Hapus HTML tags
    s = re.sub(r'<[^>]+>', ' ', s)
    # Hapus newlines dan extra whitespace
    s = ' '.join(s.replace('\n', ' ').replace('\r', '').split())
    return s.strip() or 'N/A'

def format_date(date_string):
    """Format date string ke YYYY-MM-DD HH:MM:SS."""
    if not date_string:
        return 'N/A'
    try:
        dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return date_string

def strip_html(html_string):
    """Remove HTML tags dari string."""
    if not html_string:
        return ''
    return re.sub(r'<[^>]+>', ' ', str(html_string)).strip()