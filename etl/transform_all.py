# etl/transform_all.py
import os
import glob
import pandas as pd
from pathlib import Path
from datetime import timezone, timedelta
import hashlib

from etl.transform import add_hash_and_flags, merge_with_history
from etl.load import cleanup_raw
from config import CREDITS_MAP, DEPOSITS_MAP, DROP_ALWAYS, REGIONS_MAP

RAW_BASE = "storage/data_raw"
HISTORY_BASE = "storage/history"

utc_plus_3 = timezone(timedelta(hours=3))

def read_raw(type_name: str) -> pd.DataFrame:
    """Читает все raw CSV по типу."""
    path = os.path.join(RAW_BASE, type_name)
    files = glob.glob(os.path.join(path, "**", "*.csv"), recursive=True)
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype=object)
            if df.empty:
                continue
            # auto extract region_id from folder structure
            region_id = Path(f).parts[-2]
            
            df["region_id"] = region_id
            dfs.append(df)
        except Exception as e:
            print(f"[WARN] failed to read {f}: {e}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def apply_mapping(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Переименовывает только существующие колонки."""
    rename_map = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=rename_map)


def drop_null_cols(df: pd.DataFrame, always_drop: list = None) -> pd.DataFrame:
    """Удаляем колонки, которые полностью заполнены NaN / None, 
       плюс те, которые в always_drop (если есть)."""
    if always_drop is None:
        always_drop = []
    # приводим пустые строки к NaN для надёжности
    # df = df.replace({"": pd.NA})
    # колонки полностью null
    # null_cols = [c for c in df.columns if df[c].isna().all()]
    # объединяем с always_drop, но только если колонка есть
    null_cols = [c for c in always_drop if c in df.columns]
    # null_cols = list(dict.fromkeys(null_cols))  # unique
    
    if null_cols:
        df = df.drop(columns=null_cols)
        print(f"[INFO] Dropped columns: {null_cols}")
    return df

def split_product_region(df: pd.DataFrame, uid_col="offer_uid"):
    """Разбивает DF на products и product-region таблицы."""
    df = df.copy()

    # защитная заглушка
    if uid_col not in df.columns or df[uid_col].isna().all():
        df[uid_col] = df.apply(
            lambda r: hashlib.sha256(
                f"{r.get('bank_id','')}|{r.get('offer_pledge','')}|{r.get('amountMin','')}".encode("utf-8")
            ).hexdigest(),
            axis=1,
        )

    # print(df["offer_uid"].nunique(), "уникальных offer_uid")
    # print(df[["offer_uid", "region_id"]].head())
    # print(df['region_id'].nunique(), "уникальных region_id")
    products = df.drop(columns=["region_id"]).drop_duplicates()
    region_links = df[[uid_col, "region_id"]].drop_duplicates()
    return products, region_links


#  SCD2 merge wrapper 
def merge_products(products_df: pd.DataFrame, type_name: str) -> pd.DataFrame:
    path = os.path.join(HISTORY_BASE, f"{type_name}_products_history.csv")
    return merge_with_history(products_df, history_path=path, product_type=type_name)


# def merge_product_regions(region_df: pd.DataFrame, type_name: str) -> pd.DataFrame:
#     path = os.path.join(HISTORY_BASE, f"{type_name}_regions_history.csv")
#     return merge_with_history(region_df, history_path=path, product_type="regions")

def merge_product_regions(region_df: pd.DataFrame, type_name: str) -> pd.DataFrame:
    path = os.path.join(HISTORY_BASE, f"{type_name}_regions_history.csv")
    # используем offer_uid + region_id как ключ для связи продукта и региона
    return merge_with_history(region_df, history_path=path, product_type="regions", hash_fields=["offer_uid", "region_id"])

# Transformers 
def transform_type(type_name: str, mapping: dict):
    print(f"\n=== Transform: {type_name} ===")

    raw = read_raw(type_name)
    if raw.empty:
        print(f"[INFO] No raw data for {type_name}")
        return None

    # стандартизируем колонки
    df = apply_mapping(raw, mapping)
    df = drop_null_cols(df, always_drop=DROP_ALWAYS)

    # считаем хэш до split
    # df = add_hash_and_flags(df, product_type=type_name)

    # разбиваем по продуктам / регионам
    products, regions = split_product_region(df)

    # products = add_hash_and_flags(products, product_type=type_name)
    # regions = add_hash_and_flags(regions, product_type="regions")
    # сохраняем историю
    merged_products = merge_products(products, type_name)
    merged_regions = merge_product_regions(regions, type_name)
    
    # сохраняем
    products_path = os.path.join(HISTORY_BASE, f"{type_name}_products_history.csv")
    regions_path  = os.path.join(HISTORY_BASE, f"{type_name}_regions_history.csv")
    # regions_new_path = os.path.join(HISTORY_BASE, f"regions.csv")

    # new_regions.to_csv(regions_new_path, index=False, encoding="utf-8-sig")
    merged_products.to_csv(products_path, index=False, encoding="utf-8-sig")
    merged_regions.to_csv(regions_path, index=False, encoding="utf-8-sig")
    # merged_regions.to_csv(regions_path, index=False, encoding="utf-8-sig")


    return {
        "products": len(merged_products),
        "regions": len(merged_regions)
    }

def for_regions():
    regions = pd.read_csv(os.path.join(RAW_BASE, "regions", "banki_regions_dump_.csv"))
    regions = apply_mapping(regions, REGIONS_MAP)
    regions_new = add_hash_and_flags(regions, product_type="regions")
    regions_new.to_csv(os.path.join(HISTORY_BASE, "regions.csv"), index=False, encoding="utf-8-sig")
    return
    


# main orchestrator
def transform_all():
    os.makedirs(HISTORY_BASE, exist_ok=True)

    res_credits = transform_type("credits", CREDITS_MAP)
    res_deposits = transform_type("deposits", DEPOSITS_MAP)

    for_regions()
    cleanup_raw()

    print("\n=== transform_all finished ===")
    return {
        "credits": res_credits,
        "deposits": res_deposits
    }

