import requests

url = 'https://glints.com/api/graphql'
headers = {
    'accept': 'application/json',
    'content-type': 'application/json',
    'origin': 'https://glints.com',
    'referer': 'https://glints.com/id/opportunities/jobs/explore',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'x-glints-app': 'glints-iam',
}
payload = {
    "operationName": "SearchJobs",
    "query": "query SearchJobs($data: JobSearchConditionInput!, $pagination: Pagination!) { searchJobs(data: $data, pagination: $pagination) { jobsCount jobs { title id } } }",
    "variables": { "data": { "countryCode": "ID", "lopiOnly": True, "sortBy": "LATEST" }, "pagination": { "limit": 5, "offset": 0 } }
}

r = requests.post(url, json=payload, headers=headers)
print(r.status_code)
