import requests
from config import HEADERS
from etl.transform_all import transform_all
from etl.extract import collect_new_data
from etl.db_writer import DBWriter

if __name__ == "__main__":
    print("Start")
    SESSION = requests.Session()
    SESSION.headers.update({
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.banki.ru/products/deposits/",
        "X-Requested-With": "XMLHttpRequest",
    })
    collect_new_data(take_credits=True, take_deposits=True, SESSION=SESSION)

    print("=== END collecting data ===")
    transform_all()
    print("=== END transforming data ===")
    DBWriter().run_all()
    print("=== END adding data in DB ===")