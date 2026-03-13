from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, select, func, desc, and_, asc, case
from app.recommender import reccomender
from config import config, get_latest_file
from typing import Optional
import uvicorn
import pandas as pd
import os

app = FastAPI(title="GamersBase RecSys API")
rec = reccomender()

# Инициализация SQLAlchemy
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

recommendations = Table(
    'recommendations', metadata,
    autoload_with=engine
)

user_interactions = Table(
    'user_interactions', metadata,
    autoload_with=engine
)

# Кэширование продуктов
_products_info = {}

def get_products_info():
    global _products_info
    if not _products_info:
        products_pattern = os.path.join(config.PRODUCTS_DATA_DIR, "*products*.csv")
        products_file = get_latest_file(products_pattern)
        if products_file:
            df = pd.read_csv(products_file)
            # В CSV может быть NaN в skuCode, заменим на None
            df['skuCode'] = df['skuCode'].where(pd.notnull(df['skuCode']), None)
            _products_info = df.set_index('id')[['name', 'skuCode']].to_dict('index')
    return _products_info

@app.get("/")
async def root():
    """Информация об API."""
    return {
        "message": "GamersBase Recommendation System API",
        "docs": "/docs"
    }

@app.get("/merges")
async def get_merges():
    """Список всех объединений данных."""
    # Получаем логи объединений с подсчетом количества взаимодействий через джоин
    query = (
        select(
            merge_logs, 
            func.count(user_interactions.c.merge_log_id).label('interactions_count')
        )
        .outerjoin(user_interactions, merge_logs.c.id == user_interactions.c.merge_log_id)
        .group_by(merge_logs.c.id)
        .order_by(desc(merge_logs.c.start_time))
    )
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return [dict(row._mapping) for row in result]

@app.get("/merges/{merge_id}")
async def get_merge_details(merge_id: int):
    """Детальная информация по конкретному объединению."""
    query = (
        select(
            merge_logs, 
            func.count(user_interactions.c.merge_log_id).label('interactions_count')
        )
        .outerjoin(user_interactions, merge_logs.c.id == user_interactions.c.merge_log_id)
        .where(merge_logs.c.id == merge_id)
        .group_by(merge_logs.c.id)
    )
    with engine.connect() as conn:
        merge = conn.execute(query).fetchone()
        if not merge:
            raise HTTPException(status_code=404, detail="Merge log not found")
        
        merge_data = dict(merge._mapping)
        
        # Дополнительная информация по рекомендациям
        # Считаем количество рекомендаций по моделям
        rec_query = (
            select(recommendations.c.model_name, func.count(recommendations.c.id).label('count'))
            .where(recommendations.c.merge_log_id == merge_id)
            .group_by(recommendations.c.model_name)
        )
        rec_stats = conn.execute(rec_query).fetchall()
        
        # Топ 5 пользователей с рекомендациями в этом объединении
        top_users_query = (
            select(recommendations.c.email, func.count(recommendations.c.id).label('count'))
            .where(recommendations.c.merge_log_id == merge_id)
            .group_by(recommendations.c.email)
            .order_by(desc('count'))
            .limit(5)
        )
        top_users = conn.execute(top_users_query).fetchall()

        merge_data['recommendations_stats'] = [dict(row._mapping) for row in rec_stats]
        merge_data['total_recommendations'] = sum(stat['count'] for stat in merge_data['recommendations_stats'])
        merge_data['top_users_in_recs'] = [dict(row._mapping) for row in top_users]
        
        return merge_data

@app.get("/merges/{merge_id}/users")
async def get_merge_users(
    merge_id: int, 
    limit: int = 100, 
    offset: int = 0,
    sort_by: str = Query("interactions", enum=["interactions", "email", "purchases"]),
    order: str = Query("desc", enum=["asc", "desc"]),
    search: Optional[str] = Query(None)
):
    """Список пользователей объединения с количеством взаимодействий."""
    # Выбираем пользователей и считаем взаимодействия
    query = (
        select(
            user_interactions.c.email, 
            func.count(user_interactions.c.id).label('interactions_count'),
            func.sum(case((user_interactions.c.weight == 1.0, 1), else_=0)).label('purchases_count')
        )
        .where(user_interactions.c.merge_log_id == merge_id)
        .group_by(user_interactions.c.email)
    )
    
    if search:
        query = query.where(user_interactions.c.email.ilike(f"%{search}%"))

    if sort_by == "interactions":
        order_col = desc('interactions_count') if order == "desc" else asc('interactions_count')
    elif sort_by == "purchases":
        order_col = desc('purchases_count') if order == "desc" else asc('purchases_count')
    else:
        order_col = desc(user_interactions.c.email) if order == "desc" else asc(user_interactions.c.email)
        
    query = query.order_by(order_col).limit(limit).offset(offset)
    
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        
        # Получаем общее количество пользователей для пагинации
        total_query = select(func.count(func.distinct(user_interactions.c.email))).where(user_interactions.c.merge_log_id == merge_id)
        if search:
            total_query = total_query.where(user_interactions.c.email.ilike(f"%{search}%"))
        total_count = conn.execute(total_query).scalar()
        
        return {
            "total": total_count,
            "users": [dict(row._mapping) for row in result]
        }

@app.get("/users/{email}/interactions")
async def get_user_interactions_details(email: str, merge_id: Optional[int] = Query(None)):
    """Детальные взаимодействия пользователя."""
    if merge_id is None:
        latest_merge_query = select(merge_logs.c.id).order_by(desc(merge_logs.c.start_time)).limit(1)
        with engine.connect() as conn:
            merge_id = conn.execute(latest_merge_query).scalar()

    if merge_id is None:
        raise HTTPException(status_code=404, detail="No merge logs found")

    query = (
        select(user_interactions)
        .where(and_(user_interactions.c.email == email, user_interactions.c.merge_log_id == merge_id))
        .order_by(desc(user_interactions.c.datetime))
    )
    
    products = get_products_info()
    
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        interactions = []
        for row in result:
            d = dict(row._mapping)
            p_info = products.get(d['product_id'], {"name": f"Product {d['product_id']}", "skuCode": None})
            d['product_name'] = p_info['name']
            d['skuCode'] = p_info['skuCode']
            interactions.append(d)
        
        # Разделяем на покупки и просмотры
        purchases = [i for i in interactions if i['weight'] == 1.0]
        views = [i for i in interactions if i['weight'] < 1.0]
        
        return {
            "email": email,
            "merge_id": merge_id,
            "purchases": purchases,
            "views": views
        }

@app.get("/users/{email}/recommendations")
async def get_user_recommendations_v2(email: str, merge_id: Optional[int] = Query(None)):
    """Подробные рекомендации для пользователя."""
    if merge_id is None:
        latest_merge_query = select(merge_logs.c.id).order_by(desc(merge_logs.c.start_time)).limit(1)
        with engine.connect() as conn:
            merge_id = conn.execute(latest_merge_query).scalar()

    if merge_id is None:
        raise HTTPException(status_code=404, detail="No merge logs found")

    # Получаем финальный список через рекоммендер (с учетом очереди и вытеснения)
    ranked_ids = rec.rank(email, merge_id)
    excluded_raw = rec.get_excluded()
    
    # Получаем все рекомендации из базы для этого пользователя, чтобы достать веса и модели
    query = (
        select(recommendations)
        .where(and_(recommendations.c.email == email, recommendations.c.merge_log_id == merge_id))
    )
    
    products = get_products_info()
    
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        all_recs = [dict(row._mapping) for row in result]
        
        # Обогащаем названиями и фильтруем по тем, что попали в топ
        final_recs = []
        for p_id in ranked_ids:
            # Находим данные об этой рекомендации, учитывая приоритет моделей из рекоммендера
            best_rec = None
            for model_name in rec.models_order:
                best_rec = next((r for r in all_recs if r['product_id'] == p_id and r['model_name'] == model_name), None)
                if best_rec:
                    break
            
            if best_rec:
                item = dict(best_rec)
                p_info = products.get(p_id, {"name": f"Product {p_id}", "skuCode": None})
                item['product_name'] = p_info['name']
                item['skuCode'] = p_info['skuCode']
                final_recs.append(item)
            else:
                p_info = products.get(p_id, {"name": f"Product {p_id}", "skuCode": None})
                final_recs.append({
                    "product_id": p_id,
                    "product_name": p_info['name'],
                    "skuCode": p_info['skuCode'],
                    "model_name": "unknown",
                    "score": 0.0
                })
        
        # Обогащаем исключенные рекомендации
        excluded_recs = []
        for ex in excluded_raw:
            p_id = ex['product_id']
            p_info = products.get(p_id, {"name": f"Product {p_id}", "skuCode": None})
            excluded_recs.append({
                "product_id": p_id,
                "product_name": p_info['name'],
                "skuCode": p_info['skuCode'],
                "model_name": ex['model_name']
            })
        
        return {
            "email": email,
            "merge_id": merge_id,
            "recommendations": final_recs,
            "excluded_recommendations": excluded_recs
        }

@app.get("/recommendations/{email}")
async def get_user_recommendations(email: str, merge_id: Optional[int] = Query(None)):
    """Получение готовых рекомендаций для пользователя по его email."""
    if merge_id is None:
        # Берем последнее успешное объединение
        latest_merge_query = select(merge_logs.c.id).order_by(desc(merge_logs.c.start_time)).limit(1)
        with engine.connect() as conn:
            merge_id = conn.execute(latest_merge_query).scalar()
            
    if merge_id is None:
        raise HTTPException(status_code=404, detail="No merge logs found")
    
    product_ids = rec.rank(email, merge_id)
    excluded_raw = rec.get_excluded()
    
    products = get_products_info()
    
    recs = []
    for p_id in product_ids:
        p_info = products.get(p_id, {"name": f"Product {p_id}", "skuCode": None})
        recs.append({
            "product_id": p_id,
            "product_name": p_info['name'],
            "skuCode": p_info['skuCode']
        })
        
    excluded = []
    for ex in excluded_raw:
        p_id = ex['product_id']
        p_info = products.get(p_id, {"name": f"Product {p_id}", "skuCode": None})
        excluded.append({
            "product_id": p_id,
            "product_name": p_info['name'],
            "skuCode": p_info['skuCode'],
            "model_name": ex['model_name']
        })
    
    return {
        "email": email,
        "merge_id": merge_id,
        "recommendations": recs,
        "excluded": excluded
    }

if __name__ == "__main__":
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)
