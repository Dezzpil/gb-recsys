from collections import deque
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, select, asc
from config import config

class reccomender:
    def __init__(self):
        self.engine = create_engine(config.DB_URL)
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
        self.models_order = ['similar_games', 'steam', 'user_based_cf']
        self.excluded_products = []

    def rank(self, email: str, merge_log_id: int):
        """
        Объединяет рекомендации от разных моделей в ограниченную очередь.
        Сначала similar_games, потом steam, потом user_based_cf.
        Внутри каждой модели рекомендации берутся с сортировкой по весу (score) ASC,
        чтобы при наполнении очереди элементы с меньшим весом вытеснялись первыми.
        """
        # Ограниченная очередь на 10 элементов (хранит кортежи: (product_id, model_name))
        queue = deque(maxlen=10)
        self.excluded_products = []
        seen_products = set()

        for model_name in self.models_order:
            query = (
                select(self.recommendations_table.c.product_id)
                .where(
                    (self.recommendations_table.c.email == email) &
                    (self.recommendations_table.c.model_name == model_name) &
                    (self.recommendations_table.c.merge_log_id == merge_log_id)
                )
                .order_by(asc(self.recommendations_table.c.score))
            )
            
            with self.engine.connect() as conn:
                result = conn.execute(query).fetchall()
                for row in result:
                    product_id = row.product_id
                    
                    if product_id in seen_products:
                        continue
                    
                    # Если очередь полная, при добавлении вытеснится самый старый (крайний слева) элемент.
                    # Мы сохраняем его в список исключенных.
                    if len(queue) == 10:
                        evicted_item = queue[0]
                        self.excluded_products.append({
                            "product_id": evicted_item[0],
                            "model_name": evicted_item[1]
                        })
                    
                    queue.append((product_id, model_name))
                    seen_products.add(product_id)
        
        return [item[0] for item in queue]

    def get_excluded(self):
        """Возвращает список наименований, которые были вытеснены из очереди."""
        return self.excluded_products
