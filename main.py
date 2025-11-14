# import os
# import pandas as pd
import requests
# import time
from config import COOKIES, HEADERS

from parsers.credits_parser_b import parse_credits
from parsers.deposits_parser_b import parse_deposits
from parsers.regions_parser_b import fetch_regions
from etl.extract import collect_new_data

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
    # transform_all()