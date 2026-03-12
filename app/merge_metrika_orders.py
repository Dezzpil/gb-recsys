import os
import glob
import pandas as pd
import ast
import html
import time
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime
from config import config

def get_latest_file(pattern):
    files = glob.glob(pattern)
    if not files:
        return None
    # Sort by mtime
    return max(files, key=os.path.getmtime)

def parse_products_list(s):
    if not s or s == '[]':
        return []
    try:
        # Pre-process: remove literal backslashes which might be in the CSV
        s_clean = s.replace("\\'", "'")
        # ast.literal_eval is safer than eval
        res = ast.literal_eval(s_clean)
        if isinstance(res, list):
            return [html.unescape(p) for p in res]
        return []
    except Exception:
        # If it fails, we might have some very broken string, return empty
        return []

def run_merge():
    start_time = datetime.now()
    t0 = time.time()
    
    # 1. Find files
    metrika_pattern = os.path.join(config.METRIKA_DATA_DIR, "metrika_*.csv")
    orders_pattern = os.path.join(config.ORDERS_DATA_DIR, "*orders-with-products*.csv")
    products_pattern = os.path.join(config.PRODUCTS_DATA_DIR, "*-gb-products.csv")
    
    metrika_file = get_latest_file(metrika_pattern)
    orders_file = get_latest_file(orders_pattern)
    products_file = get_latest_file(products_pattern)
    
    if not metrika_file or not orders_file or not products_file:
        print(f"Files not found. Metrika: {metrika_file}, Orders: {orders_file}, Products: {products_file}")
        return

    print(f"Metrika file: {metrika_file}")
    print(f"Orders file: {orders_file}")
    print(f"Products file: {products_file}")

    # 2. Read products list
    df_products = pd.read_csv(products_file)
    # Mapping for name -> id
    name_to_id = df_products.set_index('name')['id'].to_dict()
    # Mapping for skuId -> id
    sku_to_id = df_products.dropna(subset=['skuId']).set_index(df_products.dropna(subset=['skuId'])['skuId'].astype(int).astype(str))['id'].to_dict()

    # 3. Read orders
    df_orders = pd.read_csv(orders_file, dtype={'Last Ymcid': str, 'Email': str, 'productsSkuIds': str})
    orders_records_count = len(df_orders)
    
    # Map Email to SKU IDs for purchases (weight = 1)
    df_orders = df_orders.dropna(subset=['Email', 'productsSkuIds'])
    df_orders['sku_list'] = df_orders['productsSkuIds'].str.split(',')
    df_orders_exploded = df_orders.explode('sku_list')
    df_orders_exploded['sku_list'] = df_orders_exploded['sku_list'].str.strip()
    
    # Match skuId -> product_id
    df_orders_exploded['product_id'] = df_orders_exploded['sku_list'].map(sku_to_id)
    df_purchases = df_orders_exploded.dropna(subset=['product_id'])[['Email', 'product_id']]
    df_purchases['weight'] = 1.0
    
    # Collect missing SKU IDs for analysis
    missing_skus = df_orders_exploded[df_orders_exploded['product_id'].isna()]['sku_list'].unique()
    
    # 4. Read metrika
    df_metrika = pd.read_csv(metrika_file, dtype={'ym:s:clientID': str})
    
    # Filter only those that are in orders (for performance)
    unique_ymcids = df_orders['Last Ymcid'].dropna().unique()
    df_metrika = df_metrika[df_metrika['ym:s:clientID'].isin(unique_ymcids)]
    
    # Process metrika products
    df_metrika['products'] = df_metrika['ym:s:impressionsProductName'].apply(parse_products_list)
    df_metrika_exploded = df_metrika.explode('products')
    df_metrika_exploded = df_metrika_exploded.dropna(subset=['products'])
    
    # Merge metrika and orders to get email for views
    df_views_raw = pd.merge(
        df_orders[['Email', 'Last Ymcid']].drop_duplicates(),
        df_metrika_exploded[['ym:s:clientID', 'products']],
        left_on='Last Ymcid',
        right_on='ym:s:clientID'
    )
    
    # Match product_name -> product_id
    df_views_raw['product_id'] = df_views_raw['products'].map(name_to_id)
    df_views = df_views_raw.dropna(subset=['product_id'])[['Email', 'product_id']]
    df_views['weight'] = 0.5
    
    # Collect missing product names
    missing_names = df_views_raw[df_views_raw['product_id'].isna()]['products'].unique()

    # Save missing products for analysis
    if len(missing_skus) > 0 or len(missing_names) > 0:
        date_str = datetime.now().strftime("%Y%m%d")
        missing_file = os.path.join(config.MERGED_DATA_DIR, f"{date_str}_missing_products.txt")
        with open(missing_file, 'w') as f:
            if len(missing_skus) > 0:
                f.write("Missing SKU IDs:\n")
                f.write("\n".join(map(str, missing_skus)))
                f.write("\n\n")
            if len(missing_names) > 0:
                f.write("Missing Product Names:\n")
                f.write("\n".join(map(str, missing_names)))
        print(f"Missing products fixed in {missing_file} (SKUs: {len(missing_skus)}, Names: {len(missing_names)})")

    # 5. Combine and resolve weights (max weight per email, product_id)
    df_combined = pd.concat([df_purchases, df_views])
    df_final = df_combined.groupby(['Email', 'product_id'], as_index=False)['weight'].max()
    df_final = df_final.rename(columns={'Email': 'email'})
    df_final['product_id'] = df_final['product_id'].astype(int)
    
    # 6. Save to CSV
    # Ensure directory exists
    os.makedirs(config.MERGED_DATA_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    output_csv = os.path.join(config.MERGED_DATA_DIR, f"{date_str}_email_products.csv")
    df_final.to_csv(output_csv, index=False)
    
    unique_emails_count = df_final['email'].nunique()
    unique_products_count = df_final['product_id'].nunique()
    
    duration = time.time() - t0
    
    # 7. Log to DB (Same as before, maybe add more info)
    engine = create_engine(config.DB_URL)
    metadata = MetaData()
    merge_logs = Table(
        'merge_logs', metadata,
        Column('id', Integer, primary_key=True),
        Column('start_time', DateTime),
        Column('duration', Float),
        Column('metrika_file', String),
        Column('orders_file', String),
        Column('orders_records_count', Integer),
        Column('unique_emails_count', Integer),
        Column('unique_products_count', Integer),
        Column('merge_time', DateTime)
    )
    
    with engine.begin() as conn:
        conn.execute(merge_logs.insert().values(
            start_time=start_time,
            duration=duration,
            metrika_file=os.path.basename(metrika_file),
            orders_file=os.path.basename(orders_file),
            orders_records_count=orders_records_count,
            unique_emails_count=unique_emails_count,
            unique_products_count=unique_products_count,
            merge_time=datetime.now()
        ))
    
    print(f"Merge completed. Results saved to {output_csv}.")
    print(f"Unique emails: {unique_emails_count}, Unique products: {unique_products_count}")
    print(f"Logged to DB.")

if __name__ == "__main__":
    run_merge()
