import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, desc
from config import config
from app.models.base import BaseModel
from datetime import datetime
import glob
import os
from collections import Counter

class SimilarModel(BaseModel):
    def __init__(self):
        self.engine = create_engine(config.DB_URL)
        self.model_name = "similar_games"
        self.metadata = MetaData()
        self.recommendations_table = Table(
            'recommendations', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('email', String),
            Column('product_id', Integer),
            Column('score', Float),
            Column('model_name', String),
            Column('merge_log_id', Integer),
            Column('created_at', DateTime)
        )
        self.merge_logs_table = Table(
            'merge_logs', self.metadata,
            Column('id', Integer, primary_key=True)
        )

    def get_latest_merge_log_id(self):
        query = self.merge_logs_table.select().order_by(desc(self.merge_logs_table.c.id)).limit(1)
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            return result.id if result else None

    def fit(self, merge_log_id: int = None, top_n: int = 10):
        """
        Train the model using data from user_interactions for a specific merge_log_id.
        Finds similar games based on the 'similar' field in product CSV files.
        """
        if merge_log_id is None:
            merge_log_id = self.get_latest_merge_log_id()
            if merge_log_id is None:
                print("No merge logs found in database.")
                return

        print(f"Training Similar Games model using data from merge_log_id: {merge_log_id}")

        # 1. Load products from CSV
        # Search in data/products/ first, then assets/ as fallback
        csv_files = glob.glob(os.path.join(config.PRODUCTS_DATA_DIR, "*.csv"))
        if not csv_files:
            csv_files = glob.glob("assets/*-gb-products.csv")
            
        if not csv_files:
            print("No product CSV files found.")
            return

        all_products = []
        for f in csv_files:
            try:
                df_prod = pd.read_csv(f)
                all_products.append(df_prod)
            except Exception as e:
                print(f"Error reading {f}: {e}")
        
        if not all_products:
            print("Could not load any product data.")
            return

        products_df = pd.concat(all_products).drop_duplicates(subset=['id'])
        
        # Build name to id mapping
        name_to_id = products_df.dropna(subset=['name']).set_index('name')['id'].to_dict()
        
        # Build id to similar names mapping
        id_to_similar_names = {}
        for _, row in products_df.iterrows():
            if pd.notna(row['similar']) and row['similar'] != '':
                names = [n.strip() for n in str(row['similar']).split(',')]
                id_to_similar_names[row['id']] = names

        # 2. Load user interactions
        query = f"SELECT email, product_id, weight FROM user_interactions WHERE merge_log_id = {merge_log_id}"
        interactions_df = pd.read_sql(query, self.engine)

        if interactions_df.empty:
            print(f"No interactions found for merge_log_id {merge_log_id}")
            return

        # 3. Generate recommendations
        all_recs = []
        user_grouped = interactions_df.groupby('email')

        for email, group in user_grouped:
            user_products = set(group['product_id'].tolist())
            user_purchases = set(group[group['weight'] == 1.0]['product_id'].tolist())
            recs_counts = Counter()
            
            for pid in user_products:
                similar_names = id_to_similar_names.get(pid, [])
                for name in similar_names:
                    similar_id = name_to_id.get(name)
                    # Don't recommend products the user already purchased
                    if similar_id and similar_id not in user_purchases:
                        recs_counts[similar_id] += 1
            
            top_recs = recs_counts.most_common(top_n)
            
            for pid, count in top_recs:
                all_recs.append({
                    'email': email,
                    'product_id': int(pid),
                    'score': float(count),
                    'model_name': self.model_name,
                    'merge_log_id': merge_log_id,
                    'created_at': datetime.now()
                })

        # 4. Save to Database
        if all_recs:
            with self.engine.begin() as conn:
                # Clear previous recommendations for this model and merge_log
                conn.execute(
                    self.recommendations_table.delete().where(
                        (self.recommendations_table.c.model_name == self.model_name) &
                        (self.recommendations_table.c.merge_log_id == merge_log_id)
                    )
                )
                # Insert new ones
                conn.execute(self.recommendations_table.insert(), all_recs)
            print(f"Successfully saved {len(all_recs)} recommendations for {len(user_grouped)} users.")
        else:
            print("No recommendations were generated.")

    def predict(self, email: str, limit: int = 10):
        """
        Get recommendations for a user from the database.
        """
        latest_recs_query = (
            self.recommendations_table.select()
            .where(
                (self.recommendations_table.c.email == email) &
                (self.recommendations_table.c.model_name == self.model_name)
            )
            .order_by(desc(self.recommendations_table.c.merge_log_id), desc(self.recommendations_table.c.score))
            .limit(limit)
        )
        
        with self.engine.connect() as conn:
            result = conn.execute(latest_recs_query).fetchall()
            return [row.product_id for row in result]
