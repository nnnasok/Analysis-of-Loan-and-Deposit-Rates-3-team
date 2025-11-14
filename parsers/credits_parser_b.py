
import time
import requests
import pandas as pd
from config import COOKIES, HEADERS
from etl.load import save_to_csv


def fetch_credit_page(page=1, region_id=211, limit=25):
    url = "https://www.banki.ru/bff/catalog/api/v1/widget/group"
    params = {
        "pageType": "MAINPRODUCT_SEARCH",
        "productTypes[]": "credit",
        "requestedTermUnit": 6,
        "creditTypes[]": "money",
        "sort": "popular",
        "order": "asc",
        "page": page,
        "limit": limit,
        "regionId": region_id,
        "reason": "show_more"
    }
    resp = requests.get(url, headers=HEADERS, cookies=COOKIES, params=params, timeout=25)
    print("GET", resp.url, "->", resp.status_code)
    if resp.status_code != 200:
        print(resp.text[:200])
        return []

    data = resp.json()
    
    partners = data.get("payload", {}).get("items") or data.get("items") or []
    return partners

def fetch_credit_details(uids, max_retries=3, delay=5):
    if not uids:
        return []
    
    url = "https://www.banki.ru/bff/catalog/api/v2/products"
    params = [("uids[]", str(u)) for u in uids]
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, cookies=COOKIES, params=params, timeout=40)
            print(f"  /v2/products для {len(uids)} uid ->", resp.status_code)
            if resp.status_code == 200:
                return resp.json().get("items", [])
            else:
                print(f"[WARN] Нестандартный код {resp.status_code}: {resp.text[:200]}")
                
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"[RETRY] Ошибка при запросе деталей (попытка {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                print(f"Пауза на {delay * attempt} секунд")
                time.sleep(delay * attempt)  # пауза
            else:
                print("[FAIL] Все попытки исчерпаны")
                return []
    return []

def parse_credits(region_id=211):
    offers = []
    page = 1
    credit_dir = f'credits/{region_id}'
    
    while True:
        partners = fetch_credit_page(page, region_id)
        if not partners:
            print(f"Пусто на странице {page}")
            break

        uids = [p.get("productUid") for partner in partners for p in partner.get("items", []) if p.get("productUid")]
        if not uids:
            print("UID не найдены — выходим")
            break

        details = fetch_credit_details(uids)
        # if not details:
        #     print(f"[WARN] Не удалось получить детали для {len(uids)} uid")
        #     continue
        for prod in details:
            flat = {
                **{k: prod.get(k) for k in ["productId", "productUid", "productType", "productName", "name", "url", "smallImage", "updatedAt"]},
                **{f"partner_{k}": v for k, v in (prod.get("partner") or {}).items()},
                **{f"meta_{k}": v for k, v in (prod.get("meta") or {}).items()},
            }
            offers.append(flat)

        print(f"Страница {page}: собрано {len(offers)} предложений")
        page += 1
        time.sleep(0.5)
    # именование файла: # {SITE}_{TYPE_OFFER}_{DUMP}_{DATE}.csv DATE добавляем в сэйвере
    save_to_csv(offers, add_dir=credit_dir, filename="banki_credits_dump")

