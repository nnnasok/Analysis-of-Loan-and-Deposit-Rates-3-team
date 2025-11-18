import os, time, glob
import pandas as pd
from datetime import datetime, timezone, timedelta

# UTC+3 timezone
utc_plus_3 = timezone(timedelta(hours=3))

DATA_DIR = 'storage/data_raw/'


def cleanup_raw(days=3):
    threshold = time.time() - days * 86400
    removed = 0
    for f in glob.glob(f"{DATA_DIR}/**/*.csv", recursive=True):
        if os.path.getmtime(f) < threshold:
            os.remove(f)
            removed += 1
    print(f"[CLEANUP] removed {removed} raw files")
    return removed


def save_to_csv(data, add_dir, filename, self_dir=False):
    if not data:
        print(f"[BAD] Ничего не собрано для {add_dir} {filename}")
        return
    
    df = pd.DataFrame(data)
    datestamp = datetime.now(utc_plus_3).strftime('%Y-%m-%d')
    if self_dir:
        dir = os.path.join(DATA_DIR, add_dir)
        datestamp = ''
    os.makedirs(dir, exist_ok=True)
    # именование файла: # {SITE}_{TYPE_OFFER}_{DUMP}_{DATE}.csv
    path = os.path.join(dir, f"{filename}_{datestamp}.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[OK] Сохранено {len(df)} строк в {filename}")


def to_db(df, table_name, db_path="storage/banki_data.db"):
    """
    Сохраняет DataFrame в SQL базу данных.
    Пока не реализована.
    Заглушка.
    """
    return
    # import sqlite3
    # conn = sqlite3.connect(db_path)
    # df.to_sql(table_name, conn, if_exists="replace", index=False)
    # conn.close()
    # print(f"[OK] {table_name} записана в {db_path}")