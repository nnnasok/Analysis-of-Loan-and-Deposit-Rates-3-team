import os, time, glob
import pandas as pd
from datetime import datetime, timezone, timedelta

# UTC+3 timezone
utc_plus_3 = timezone(timedelta(hours=3))

DATA_DIR = 'storage/data_raw/'


def cleanup_raw_data(base_dir="storage/data_raw", days=10):
    """
    Удаляет csv-файлы старше заданного количества дней.
    Т.к. мы их к тому времени уже зальем в бд, они нам в сыром виде не нужны.
    Пока не встроен в пайлайн, т.к. бд нет.
    """
    threshold = time.time() - days * 86400
    for file in glob.glob(f"{base_dir}/**/*.csv", recursive=True):
        if os.path.getmtime(file) < threshold:
            os.remove(file)
            print(f"[CLEANUP] Удалён старый файл: {file}")


def save_to_csv(data, add_dir, filename, self_dir=None):
    if not data:
        print(f"[BAD] Ничего не собрано для {add_dir} {filename}")
        return
    
    df = pd.DataFrame(data)
    datestamp = datetime.now(utc_plus_3).strftime('%Y-%m-%d')
    if not self_dir:
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