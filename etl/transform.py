import pandas as pd
import hashlib
from datetime import datetime, timezone, timedelta
import os 
# UTC+3 timezone
utc_plus_3 = timezone(timedelta(hours=3))

def compute_hash(row, fields):
    """создаём хэш на основе значимых полей."""
    concat = "|".join(str(row.get(f)) for f in fields)
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()

def add_hash_and_flags(df: pd.DataFrame, hash_fields=None, product_type: str="credit") -> pd.DataFrame:
    """
    добавляет product_hash, is_actual, start_time, end_time.
    хэш используется, чтобы понимать: это то же самое предложение или новое на основе одной колонки
    """
    if hash_fields is None:
        if product_type == "credits":
            # для кредитов можно: ставка + сумма + банк + срок
            # нужно нормализовать поля для кредитов и вкладов (upd: не нужно)
            hash_fields = ['offer_id', 'offer_type', 'offer_pledge', 'bank_id', 'bank_uid', 'rateMin', 'rateMax', 'amountMin', 'amountMax', 'termMin', 'termMax', 'periodToNotation']

        elif product_type == "deposits":
            # поменять
            hash_fields = [
                "bank_name",
                "rate_min", "rate_max",
                "amount_from", "amount_to",
                "period_from", "period_to"
            ]
        elif product_type == 'regions':
            hash_fields = [
                "region_url", 
                "name",
            ]
        else:
            raise ValueError(f"Неизвестный product_type: {product_type}")
        
        
    df = df.copy()
    df["product_hash"] = df.apply(lambda row: compute_hash(row, hash_fields), axis=1)
    df["is_actual"] = True
    df["start_time"] = datetime.now(utc_plus_3).strftime("%Y-%m-%d %H:%M:%S")
    df["end_time"] = None
    return df

def merge_with_history(new_df: pd.DataFrame, history_path: str, product_type: str="credit") -> pd.DataFrame:
    """
    Добавляет хэши и флаги к новому набору данных.
    Сравнивает новый набор данных с историческим CSV.
    Помечает неактуальные записи и добавляет новые.
    """
    new_df = add_hash_and_flags(new_df, product_type=product_type)
    try:
        old_df = pd.read_csv(history_path)
        print(f"[INFO] Загружена история: {len(old_df)} записей")
    except FileNotFoundError:
        print("[INFO] История не найдена, создаём новую")
        return new_df

    # совпадающие по хэшу остаются актуальными
    merged = pd.concat([old_df, new_df])
    merged.drop_duplicates(subset=["product_hash"], keep="last", inplace=True)

    # все старые, которых нет в новом датафрейме, считаем устаревшими
    old_hashes = set(old_df["product_hash"])
    new_hashes = set(new_df["product_hash"])
    inactive = old_hashes - new_hashes

    merged.loc[merged["product_hash"].isin(inactive), "is_actual"] = False
    merged.loc[merged["product_hash"].isin(inactive), "end_time"] = datetime.now(utc_plus_3).strftime("%Y-%m-%d %H:%M:%S")

    print(f"[INFO] Обновлено типа {product_type}: актуальных {len(new_hashes)}, устаревших {len(inactive)}")
    return merged

