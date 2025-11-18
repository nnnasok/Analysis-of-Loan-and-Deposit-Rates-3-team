import requests
import pandas as pd

from etl.load import save_to_csv

def fetch_regions(limit=100, dir='regions'):
    url = f'https://www.banki.ru/products/api/cities/top/?limit={limit}'
    resp = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    }, timeout=15)
    print('GET', resp.url, '->', resp.status_code)
    if resp.status_code != 200:
        print(resp.text[:300])
        return []
    data = resp.json().get('data', [])
    df = pd.DataFrame(data)
    save_to_csv(data, add_dir=dir, filename='banki_regions_dump')
    return df
