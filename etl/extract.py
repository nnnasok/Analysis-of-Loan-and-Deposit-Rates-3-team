from parsers.credits_parser_b import parse_credits
from parsers.deposits_parser_b import parse_deposits
from parsers.regions_parser_b import fetch_regions
import time
import random 

def collect_new_data(take_credits=True, take_deposits=True, SESSION=None):
    # fetch_regions -> pd.DataFrame 
    regions = fetch_regions()
    
    if len(regions) > 0:
        try:
            for id, city in zip(regions['id'], regions['region_url']):
                if take_credits:
                    try:
                        parse_credits(region_id=id)
                    except Exception as e:
                        print(f"[ERROR] Регион по кредиту {id} не обработан: {e}")
                        continue
                    
                if take_deposits:
                    try:
                        parse_deposits(SESSION, city=city, region_id=id)
                    except Exception as e:
                        print(f"[ERROR] Регион по депозиту {id} не обработан: {e}")
                        continue
                    
                print(f"\n=== End with {city}, {id} ===\n")
                time.sleep(random.uniform(1.5, 4.0)) # чтобы не нагружать сервер


        except ValueError:
            print(f"[BAD] Изменение полей id, regions_url таблицы. \nАктуальные поля: {list(regions.columns())}")
    else:
        print(f"[BAD] Не удалось получить список регионов. \nТекущий regions: {regions}")

