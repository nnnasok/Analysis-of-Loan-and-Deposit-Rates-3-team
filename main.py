import requests
from config import HEADERS
from etl.transform_all import transform_all
from etl.extract import collect_new_data
from etl.db_writer import DBWriter

if __name__ == "__main__":

    print("Start")
    collect_new_data(take_credits=True, take_deposits=True)  
    print("=== END collecting data ===")
    transform_all()
    print("=== END transforming data ===")
    DBWriter().run_all()
    print("=== END adding data in DB ===")