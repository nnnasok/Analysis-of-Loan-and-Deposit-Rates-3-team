import os
import glob
import pandas as pd
from etl.transform import merge_with_history
from etl.load import save_to_csv

DATA_RAW = "storage/data_raw"
DATA_HISTORY = "storage/history"

def get_latest_csvs(folder: str, pattern="*.csv", days=2):
    """Берёт все CSV за последние days дней из указанной папки."""

    # glob.glob возвращает список путей, соответствующих шаблону
    files = glob.glob(os.path.join(folder, "**", pattern), recursive=True)
    if not files:
        return []
    
    # os.path.getmtime - время последней модификации файла
    files = sorted(files, key=os.path.getmtime, reverse=True)
    return files

def read_and_concat(files):
    """Считывает список CSV и объединяет в один DataFrame."""
    dfs = []
    for file in files:
        try:
            df = pd.read_csv(file)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"[WARN] Не удалось прочитать {file}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def transform_all():
    pass
