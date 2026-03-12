import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DB_URL = os.getenv("DB_URL")
    
    METRIKA_COUNTER_ID = os.getenv("METRIKA_COUNTER_ID")
    METRIKA_OAUTH_TOKEN = os.getenv("METRIKA_OAUTH_TOKEN")
    METRIKA_COLUMNS = os.getenv("METRIKA_COLUMNS", "ym:s:clientID,ym:s:impressionsDateTime,ym:s:impressionsProductName").split(",")
    METRIKA_DATA_DIR = os.getenv("METRIKA_DATA_DIR")

    ORDERS_DATA_DIR = os.getenv("ORDERS_DATA_DIR")
    MERGED_DATA_DIR = os.getenv("MERGED_DATA_DIR")
    PRODUCTS_DATA_DIR = os.getenv("PRODUCTS_DATA_DIR", "data/products")

config = Config()
