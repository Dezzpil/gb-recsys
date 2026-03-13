import os
import sys
import time
import traceback
import argparse
from datetime import datetime

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, text
from config import config
from app.export_metrika import MetrikaExporter
from app.merge_metrika_orders import run_merge
from app.models.cf import CFModel
from app.models.similar import SimilarModel
from app.models.steam import SteamModel

def log_training_to_db(engine, model_name, merge_log_id, start_time, duration, recommendations_count, status, error_message=None):
    metadata = MetaData()
    training_logs = Table(
        'model_training_logs', metadata,
        Column('id', Integer, primary_key=True),
        Column('model_name', String),
        Column('merge_log_id', Integer),
        Column('start_time', DateTime),
        Column('duration', Float),
        Column('recommendations_count', Integer),
        Column('status', String),
        Column('error_message', String),
        Column('created_at', DateTime)
    )
    
    with engine.begin() as conn:
        conn.execute(training_logs.insert().values(
            model_name=model_name,
            merge_log_id=merge_log_id,
            start_time=start_time,
            duration=duration,
            recommendations_count=recommendations_count,
            status=status,
            error_message=error_message,
            created_at=datetime.now()
        ))

def get_recommendations_count(engine, model_name, merge_log_id):
    query = text("SELECT COUNT(*) FROM recommendations WHERE model_name = :model AND merge_log_id = :merge_id")
    with engine.connect() as conn:
        result = conn.execute(query, {"model": model_name, "merge_id": merge_log_id}).scalar()
        return result or 0

def run_pipeline(skip_metrika=False, start_date=None, end_date=None, max_steam_games=None):
    engine = create_engine(config.DB_URL)
    pipeline_start_time = time.time()
    
    print("="*60)
    print(f"PIPELINE STARTED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 1. Export from Metrika
    if not skip_metrika:
        print("\n[STEP 1/3] Exporting data from Yandex Metrika...")
        try:
            exporter = MetrikaExporter()
            exporter.run_export(start_date, end_date)
        except Exception as e:
            print(f"Error during Metrika export: {e}")
            # We continue because we might have old files to merge
    else:
        print("\n[STEP 1/3] Skipping Metrika export (as requested).")
    
    # 2. Merge data
    print("\n[STEP 2/3] Merging Metrika and Orders data...")
    try:
        merge_log_id = run_merge()
        if not merge_log_id:
            print("Pipeline stopped: Merging failed (no merge_log_id returned).")
            return
    except Exception as e:
        print(f"Error during data merging: {e}")
        traceback.print_exc()
        return
    
    # 3. Train models
    print(f"\n[STEP 3/3] Training models for merge_log_id: {merge_log_id}...")
    
    models = [
        CFModel(),
        SimilarModel(),
        SteamModel()
    ]
    
    for model in models:
        model_name = getattr(model, 'model_name', model.__class__.__name__)
        print(f"\n--- Training {model_name} ---")
        
        start_time = datetime.now()
        t0 = time.time()
        
        try:
            # Most models have default params in fit()
            if model_name == "steam":
                model.fit(merge_log_id=merge_log_id, max_games=max_steam_games)
            else:
                model.fit(merge_log_id=merge_log_id)
            
            duration = time.time() - t0
            recs_count = get_recommendations_count(engine, model_name, merge_log_id)
            
            status = "success"
            
            # Special case for Steam model which might skip if API is down
            if model_name == "steam" and recs_count == 0:
                 # Check if it was really a skip or just no recommendations
                 # In SteamModel, fit returns early if API not alive. 
                 # We can't easily check internal state, but if duration is very short and recs 0, it's likely a skip.
                 if duration < 5: 
                     status = "skipped"
            
            log_training_to_db(
                engine, 
                model_name, 
                merge_log_id, 
                start_time, 
                duration, 
                recs_count, 
                status
            )
            print(f"Completed {model_name} in {duration:.2f}s. Generated {recs_count} recommendations.")
            
        except Exception as e:
            duration = time.time() - t0
            error_msg = str(e) + "\n" + traceback.format_exc()
            print(f"Error training {model_name}: {e}")
            
            log_training_to_db(
                engine, 
                model_name, 
                merge_log_id, 
                start_time, 
                duration, 
                0, 
                "failed",
                error_msg
            )

    total_duration = time.time() - pipeline_start_time
    print("\n" + "="*60)
    print(f"PIPELINE FINISHED SUCCESSFULLY IN {total_duration:.2f}s")
    print("="*60)
    
    # Summary of training logs
    print("\nTraining Summary:")
    try:
        query = text("SELECT model_name, status, duration, recommendations_count FROM model_training_logs WHERE merge_log_id = :merge_id")
        with engine.connect() as conn:
            results = conn.execute(query, {"merge_id": merge_log_id}).fetchall()
            for r in results:
                print(f"- {r.model_name:15} | Status: {r.status:10} | Time: {r.duration:7.2f}s | Recs: {r.recommendations_count}")
    except Exception as e:
        print(f"Error fetching summary: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run full Recommender System Pipeline')
    parser.add_argument('--skip-metrika', action='store_true', help='Skip Yandex Metrika export step')
    parser.add_argument('--start', type=str, help='Metrika start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='Metrika end date (YYYY-MM-DD)')
    parser.add_argument('--max-steam-games', type=int, help='Limit number of games for Steam model training (for testing)')
    
    args = parser.parse_args()
    
    run_pipeline(skip_metrika=args.skip_metrika, start_date=args.start, end_date=args.end, max_steam_games=args.max_steam_games)
