import pandas as pd
import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, desc
from config import config, get_latest_file
from app.models.base import BaseModel
from datetime import datetime
import requests
import time
import threading
from fastapi import FastAPI
import uvicorn
import logging

# Disable uvicorn access logging to keep output clean
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

class SteamModel(BaseModel):
    def __init__(self):
        self.engine = create_engine(config.DB_URL)
        self.model_name = "steam"
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
        self.api_url = config.STEAM_API_URL
        self.swagger_url = config.STEAM_SWAGGER_URL
        self.callback_port = config.STEAM_CALLBACK_PORT
        self.callback_path = "/callback"
        self.callback_url = f"http://127.0.0.1:{self.callback_port}{self.callback_path}"
        self.results = {}
        self.results_lock = threading.Lock()
        self.pending_games = set()
        self.server_started = False

    def get_latest_merge_log_id(self):
        query = self.merge_logs_table.select().order_by(desc(self.merge_logs_table.c.id)).limit(1)
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            return result.id if result else None

    def check_api_alive(self):
        try:
            response = requests.get(self.swagger_url, timeout=5)
            return response.status_code == 200
        except Exception as e:
            print(f"Steam API is not alive: {e}")
            return False

    def start_callback_server(self):
        if self.server_started:
            return
            
        app = FastAPI()

        @app.post(self.callback_path)
        async def callback(payload: dict):
            with self.results_lock:
                results = payload.get("results", {})
                if results:
                    print(f"\nReceived callback for {len(results)} games.")
                for game, similar_games in results.items():
                    game_key = game.lower().strip()
                    self.results[game_key] = similar_games
                    if game_key in self.pending_games:
                        self.pending_games.remove(game_key)
            return {"status": "ok"}

        def run_server():
            config_uvicorn = uvicorn.Config(app, host="0.0.0.0", port=self.callback_port, log_level="error")
            server = uvicorn.Server(config_uvicorn)
            server.run()

        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        self.server_started = True
        # Give server a moment to start
        time.sleep(1)

    def fit(self, merge_log_id: int = None, top_n: int = 10, max_games: int = None):
        if merge_log_id is None:
            merge_log_id = self.get_latest_merge_log_id()
            if merge_log_id is None:
                print("No merge logs found in database.")
                return

        if not self.check_api_alive():
            print("Steam API is not available. Skipping training.")
            return

        print(f"Training Steam model using data from merge_log_id: {merge_log_id}")

        # 1. Load interactions
        query = f"SELECT email, product_id, weight FROM user_interactions WHERE merge_log_id = {merge_log_id}"
        interactions_df = pd.read_sql(query, self.engine)
        
        if interactions_df.empty:
            print(f"No interactions found for merge_log_id {merge_log_id}")
            return

        # 2. Load products catalog
        products_pattern = os.path.join(config.PRODUCTS_DATA_DIR, "*products*.csv")
        products_file = get_latest_file(products_pattern)
        if not products_file:
            print("No product CSV catalog found.")
            return

        print(f"Loading products from: {products_file}")
        products_df = pd.read_csv(products_file)
        product_id_to_name = products_df.set_index('id')['name'].to_dict()
        # Normalized map for matching
        product_name_to_id = {str(name).lower().strip(): pid for name, pid in products_df.set_index('name')['id'].to_dict().items()}

        # 3. Filter games interacted by users
        interacted_games_df = interactions_df.copy()
        interacted_games_df['product_name'] = interacted_games_df['product_id'].map(product_id_to_name)
        
        all_games_to_search = interacted_games_df['product_name'].dropna().unique().tolist()
        
        if max_games:
            print(f"Limiting search to {max_games} unique games for testing.")
            all_games_to_search = all_games_to_search[:max_games]
        
        if not all_games_to_search:
            print("No interacted games found to search for similar ones.")
            return

        # Start callback server
        self.start_callback_server()
        
        # 4. Batch requests to Steam API
        batch_size = 10
        for i in range(0, len(all_games_to_search), batch_size):
            batch = all_games_to_search[i:i + batch_size]
            batch_normalized = [str(g).lower().strip() for g in batch]
            with self.results_lock:
                self.pending_games.update(batch_normalized)
            
            payload = {
                "games": batch,
                "callbackUrl": self.callback_url
            }
            try:
                resp = requests.post(self.api_url, json=payload, timeout=10)
                if resp.status_code != 202:
                    print(f"Failed to start search for batch: {resp.status_code}")
                    with self.results_lock:
                        for g in batch_normalized:
                            self.pending_games.discard(g)
            except Exception as e:
                print(f"Error sending request to Steam API: {e}")
                with self.results_lock:
                    for g in batch_normalized:
                        self.pending_games.discard(g)

        # 5. Wait for all callback results
        timeout = 600  # 10 minutes
        start_wait = time.time()
        while True:
            with self.results_lock:
                if not self.pending_games:
                    break
            if time.time() - start_wait > timeout:
                print("Timeout reached waiting for Steam API callback results.")
                break
            print(f"Waiting for results... {len(self.pending_games)} games pending.")
            time.sleep(5)

        # 6. Generate Recommendations
        all_recs = []
        user_groups = interacted_games_df.groupby('email')
        
        for email, group in user_groups:
            user_interacted_ids = set(group['product_id'].tolist())
            user_interacted_names = group['product_name'].dropna().tolist()
            
            user_recs_scores = {}
            
            for game_name in user_interacted_names:
                game_key = str(game_name).lower().strip()
                similar_games = self.results.get(game_key, [])
                for idx, item in enumerate(similar_games):
                    title = str(item.get('title')).lower().strip()
                    if title in product_name_to_id:
                        pid = product_name_to_id[title]
                        if pid in user_interacted_ids:
                            continue
                        
                        # Weight calculation:
                        # Frequency of appearance is primary.
                        # Position bonus (small) ensures order within response is respected.
                        score = 1.0 + (1.0 / (idx + 1))
                        user_recs_scores[pid] = user_recs_scores.get(pid, 0) + score
            
            # Sort by score descending and take top_n
            sorted_recs = sorted(user_recs_scores.items(), key=lambda x: x[1], reverse=True)[:top_n]
            
            for pid, score in sorted_recs:
                all_recs.append({
                    'email': email,
                    'product_id': int(pid),
                    'score': float(score),
                    'model_name': self.model_name,
                    'merge_log_id': merge_log_id,
                    'created_at': datetime.now()
                })

        # 7. Save to Database
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
            print(f"Successfully saved {len(all_recs)} recommendations for model '{self.model_name}'.")
        else:
            print("No recommendations were generated for Steam model.")

    def predict(self, email: str, limit: int = 5):
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
