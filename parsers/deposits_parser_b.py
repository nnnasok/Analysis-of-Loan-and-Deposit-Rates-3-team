import time
import requests
import pandas as pd
from etl.load import save_to_csv
from config import HEADERS

# вклады
def fetch_deposits_page(SESSION, page=1, city="sankt-peterburg", per_page=10):
    url = "https://www.banki.ru/depositsNodejsApi/getSearchResults/"
    # SESSION = requests.Session()
    # SESSION.headers.update({
    #     "User-Agent": HEADERS["User-Agent"],
    #     "Accept": "application/json, text/plain, */*",
    #     "Referer": "https://www.banki.ru/products/deposits/",
    #     "X-Requested-With": "XMLHttpRequest",
    # })
    params = {
        "amount": 0,
        "currency": "RUB",
        "period": 0,
        "top_hundred_place": 0,
        "partial_withdrawal": 0,
        "replenishment": 0,
        "payment_period_per_month": 0,
        "capitalization": 0,
        "early_termination_method": 0,
        "is_no_additional_expenses": 0,
        "is_only_bankiru_offer": 0,
        "type": "All",
        "page": page,
        "pageMarketPlace": 1,
        "is_main_page": 1,
        "segmentation_page": "deposits_main",
        "city": city,
        "sort": "popular",
        "order": "desc",
        "per_page": per_page,
        "page_type": "MAINPRODUCT_SEARCH",
        "aff_sub2": "/products/deposits/",
    }
    resp = SESSION.get(url, params=params, timeout=20)
    print("GET", resp.url, "->", resp.status_code)
    if resp.status_code != 200:
        print(resp.text[:200])
        return []
    data = resp.json()
    return data.get("results") or data.get("result") or []

def parse_deposits(SESSION, city="sankt-peterburg", region_id=None):
    if region_id:
        deposit_dir = f'deposits/{region_id}'
    else:
        print("region_id не задан, используем city")
        deposit_dir = f'deposits/{city}'
    offers = []
    page = 1
    while True:
        results = fetch_deposits_page(SESSION, page, city)
        if not results:
            print(f"Пусто на странице {page}")
            break

        for bank in results:
            for dep in bank.get("deposit_result_rows", []):
                offers.append({
                    "bank_id": dep.get("bank_id"),
                    "bank_name": dep.get("bank_name"),
                    "bank_logo": dep.get("bank_logo"),
                    "bank_licence": dep.get("bank_licence"),
                    "product_name": dep.get("product_name"),
                    "product_url": f"https://www.banki.ru{dep.get('product_url')}",
                    "rate_min": dep.get("rate_min"),
                    "rate_max": dep.get("rate_max"),
                    "amount_from": dep.get("amount_from"),
                    "amount_to": dep.get("amount_to"),
                    "period_from": dep.get("period_from"),
                    "period_to": dep.get("period_to"),
                    "currency": dep.get("currency_code"),
                    "efficient_rate": dep.get("efficient_rate"),
                    "is_special_offer": dep.get("is_special_offer"),
                    "is_online_opening_possible": dep.get("is_online_opening_possible"),
                    "is_partial_withdrawal_possible": dep.get("is_partial_withdrawal_possible"),
                    "is_replenishment_possible": dep.get("is_replenishment_possible"),
                    "action_title": dep.get("action_title"),
                    "action_percent": dep.get("action_percent"),
                    "action_link": dep.get("action_link"),
                    "capitalization": dep.get("capitalization", {}).get("name"),
                    "early_termination_method": dep.get("early_termination_method", {}).get("name"),
                    "rates_min": dep.get("rates_extremum", {}).get("min_rate"),
                    "rates_max": dep.get("rates_extremum", {}).get("max_rate"),
                })
        print(f"Страница {page}: собрано {len(offers)} предложений")
        page += 1
        time.sleep(0.5)

    save_to_csv(offers, add_dir=deposit_dir, filename=f"banki_deposits_dump")