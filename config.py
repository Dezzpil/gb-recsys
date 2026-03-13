import os
import glob
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

    STEAM_API_URL = os.getenv("STEAM_API_URL", "http://127.0.0.1:3000/api/search-similar")
    STEAM_SWAGGER_URL = os.getenv("STEAM_SWAGGER_URL", "http://127.0.0.1:3000/swagger.json")
    STEAM_CALLBACK_PORT = int(os.getenv("STEAM_CALLBACK_PORT", "3001"))

    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "4000"))

config = Config()

def get_latest_file(pattern):
    files = glob.glob(pattern)
    if not files:
        return None
    # Sort by mtime
    return max(files, key=os.path.getmtime)
