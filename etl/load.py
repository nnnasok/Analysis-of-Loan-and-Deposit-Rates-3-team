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
    dir = os.path.join(DATA_DIR, add_dir)
    if self_dir:
        datestamp = ''
    os.makedirs(dir, exist_ok=True)
    # именование файла: # {SITE}_{TYPE_OFFER}_{DUMP}_{DATE}.csv
    path = os.path.join(dir, f"{filename}_{datestamp}.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[OK] Сохранено {len(df)} строк в {filename}")
