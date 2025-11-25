# from parsers.credits_parser_b import parse_credits
# from parsers.deposits_parser_b import parse_deposits
# from parsers.regions_parser_b import fetch_regions
# import time
# import random 

# def collect_new_data(take_credits=True, take_deposits=True, SESSION=None):
#     # fetch_regions -> pd.DataFrame 
#     regions = fetch_regions()
    
#     if len(regions) > 0:
#         try:
#             for id, city in zip(regions['id'], regions['region_url']):
#                 if take_credits:
#                     try:
#                         parse_credits(region_id=id)
#                     except Exception as e:
#                         print(f"[ERROR] Регион по кредиту {id} не обработан: {e}")
#                         continue
                    
#                 if take_deposits:
#                     try:
#                         parse_deposits(SESSION, city=city, region_id=id)
#                     except Exception as e:
#                         print(f"[ERROR] Регион по депозиту {id} не обработан: {e}")
#                         continue
                    
#                 print(f"\n=== End with {city}, {id} ===\n")
#                 time.sleep(random.uniform(1.5, 4.0)) # чтобы не нагружать сервер


#         except ValueError:
#             print(f"[BAD] Изменение полей id, regions_url таблицы. \nАктуальные поля: {list(regions.columns())}")
#     else:
#         print(f"[BAD] Не удалось получить список регионов. \nТекущий regions: {regions}")

import asyncio
import random
import time
import pandas as pd

from parsers.async_credits_parser import parse_credits
from parsers.async_deposits_parser import parse_deposits
from parsers.regions_parser_b import fetch_regions


def collect_new_data(take_credits=True, take_deposits=True):
    # fetch_regions -> pd.DataFrame
    regions_df = fetch_regions()
    if regions_df.empty:
        print("[BAD] Не удалось получить список регионов")
        return

    # подготовим список регионов для async-парсеров
    regions_list = [
        {"region_id": r_id, "city": city}
        for r_id, city in zip(regions_df["id"], regions_df["region_url"])
    ]

    if take_credits or take_deposits:
        async def main():
            tasks = []
            if take_credits:
                tasks.append(parse_credits(regions_df["id"].tolist()))  # список ID регионов
            if take_deposits:
                tasks.append(parse_deposits(regions_list[:10]))  # список dicts с id и city
            if tasks:
                await asyncio.gather(*tasks)

        # синхронный wrapper для main.py / VS Code
        asyncio.run(main())

    print("[INFO] Все данные по регионам спаршены")
