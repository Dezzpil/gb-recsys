import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, desc
from config import config
from app.models.base import BaseModel
from datetime import datetime

class CFModel(BaseModel):
    def __init__(self):
        self.engine = create_engine(config.DB_URL)
        self.model_name = "user_based_cf"
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

    def fit(self, merge_log_id: int = None, top_n: int = 5, neighbor_count: int = 5, neighbor_thresh: float = 0.3):
        """
        Train the model using data from user_interactions for a specific merge_log_id.
        If merge_log_id is None, use the latest one.
        """
        if merge_log_id is None:
            merge_log_id = self.get_latest_merge_log_id()
            if merge_log_id is None:
                print("No merge logs found in database.")
                return

        print(f"Training CF model using data from merge_log_id: {merge_log_id}")

        # 1. Load data
        query = f"SELECT email, product_id, weight FROM user_interactions WHERE merge_log_id = {merge_log_id}"
        df = pd.read_sql(query, self.engine)

        if df.empty:
            print(f"No interactions found for merge_log_id {merge_log_id}")
            return

        # 2. Create User-Item Matrix
        # Note: we use weight as value. Purchases = 1.0, Views = 0.5
        user_item_matrix = df.pivot_table(index='email', columns='product_id', values='weight', fill_value=0)
        
        if user_item_matrix.empty:
            print("User-item matrix is empty after pivot.")
            return

        # 3. Calculate User Similarity
        user_similarity = cosine_similarity(user_item_matrix)
        user_similarity_df = pd.DataFrame(user_similarity, index=user_item_matrix.index, columns=user_item_matrix.index)

        # 4. Generate Recommendations
        all_recs = []
        emails = user_item_matrix.index.tolist()
        
        for user_email in emails:
            # Find similar users
            similar_users_series = user_similarity_df[user_email].drop(user_email).sort_values(ascending=False).head(neighbor_count)
            # Filter by threshold
            similar_users = similar_users_series[similar_users_series > neighbor_thresh].index.tolist()
            
            if not similar_users:
                continue
                
            # Get products from similar users
            similar_users_data = user_item_matrix.loc[similar_users]
            
            # Weighted sum of scores
            # scores = similar_users_data.sum(axis=0) # As in notebook
            
            # Better: weighted sum by similarity
            # weights = similar_users_series[similar_users]
            # scores = (similar_users_data.T * weights).T.sum(axis=0)
            
            # To be consistent with the notebook's "experience":
            scores = similar_users_data.sum(axis=0)

            # Exclude products already "consumed" by the user
            user_consumed = user_item_matrix.loc[user_email]
            recommendations = scores[user_consumed == 0].sort_values(ascending=False).head(top_n)
            
            # Filter out zero scores
            recommendations = recommendations[recommendations > 0]
            
            for pid, score in recommendations.items():
                all_recs.append({
                    'email': user_email,
                    'product_id': int(pid),
                    'score': float(score),
                    'model_name': self.model_name,
                    'merge_log_id': merge_log_id,
                    'created_at': datetime.now()
                })

        # 5. Save to Database
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
            print(f"Successfully saved {len(all_recs)} recommendations for {len(emails)} users.")
        else:
            print("No recommendations were generated.")

    def predict(self, email: str, limit: int = 5):
        """
        Get recommendations for a user from the database.
        Uses the latest training results for this model.
        """
        # Find the latest merge_log_id for which we have recommendations for this model
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
