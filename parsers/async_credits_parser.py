import asyncio
import aiohttp
import json
import os
from etl.load import save_to_csv
from config import HEADERS
from dotenv import load_dotenv

load_dotenv()

cookies_str = os.getenv("COOKIES")
if cookies_str:
    COOKIES = json.loads(cookies_str)
else:
    print("[WARN] COOKIES not found in .env, using empty cookies")
    COOKIES = {}

SEM_LIMIT = 10  # ограничение одновременных запросов

async def fetch_credit_page(session, page=1, region_id=211, limit=25):
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

    async with session.get(url, params=params, timeout=25) as resp:
        print("GET", str(resp.url), "->", resp.status)
        if resp.status != 200:
            text = await resp.text()
            print(text[:200])
            return []
        data = await resp.json()
        partners = data.get("payload", {}).get("items") or data.get("items") or []
        return partners


async def fetch_credit_details(session, uids, max_retries=3, delay=5):
    if not uids:
        return []

    url = "https://www.banki.ru/bff/catalog/api/v2/products"
    params = [("uids[]", str(u)) for u in uids]

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, params=params, timeout=40) as resp:
                print(f"/v2/products для {len(uids)} uid ->", resp.status)
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("items", [])
                else:
                    text = await resp.text()
                    print(f"[WARN] Нестандартный код {resp.status}: {text[:200]}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"[RETRY] Ошибка запроса деталей (попытка {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(delay * attempt)
            else:
                print("[FAIL] Все попытки исчерпаны")
                return []
    return []


async def parse_credits_region(session, region_id=211):
    offers = []
    page = 1
    credit_dir = f'credits/{region_id}'
    os.makedirs(credit_dir, exist_ok=True)
    sem = asyncio.Semaphore(SEM_LIMIT)

    while True:
        async with sem:
            partners = await fetch_credit_page(session, page, region_id)
        if not partners:
            print(f"Пусто на странице {page}")
            break

        uids = [p.get("productUid") for partner in partners for p in partner.get("items", []) if p.get("productUid")]
        if not uids:
            print("UID не найдены — выходим")
            break

        details = await fetch_credit_details(session, uids)

        for prod in details:
            flat = {
                **{k: prod.get(k) for k in ["productId", "productUid", "productType", "productName", "name", "url", "smallImage", "updatedAt"]},
                **{f"partner_{k}": v for k, v in (prod.get("partner") or {}).items()},
                **{f"meta_{k}": v for k, v in (prod.get("meta") or {}).items()},
            }
            offers.append(flat)

        print(f"Страница {page}: собрано {len(offers)} предложений")
        page += 1
        await asyncio.sleep(0.5)

    save_to_csv(offers, add_dir=credit_dir, filename="banki_credits_dump")


# Асинхронная обёртка для всех регионов
async def parse_credits(regions_list):
    """
    regions_list: list of region_id
    """
    async with aiohttp.ClientSession(cookies=COOKIES, headers=HEADERS) as session:
        sem = asyncio.Semaphore(SEM_LIMIT)
        tasks = [parse_credits_region(session, region_id=r_id) for r_id in regions_list]
        await asyncio.gather(*tasks)
