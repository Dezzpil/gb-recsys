from sqlalchemy import text
from app.models.steam import SteamModel

def main():
    model = SteamModel()
    
    # 1. Get the latest merge_log_id
    latest_log_id = model.get_latest_merge_log_id()
    if latest_log_id is None:
        print("Error: No merge logs found. Run merge_metrika_orders.py first.")
        return
        
    print(f"Latest merge log ID: {latest_log_id}")
    
    # 2. Train the model
    # Note: Training will wait for Steam API results
    model.fit(merge_log_id=latest_log_id, top_n=10)
    
    # 3. Test prediction for a user from the dataset
    with model.engine.connect() as conn:
        res = conn.execute(text(f"SELECT email FROM user_interactions WHERE weight = 1.0 AND merge_log_id = {latest_log_id} LIMIT 10")).fetchall()
        if res:
            for row in res:
                test_email = row[0]
                recs = model.predict(test_email)
                if recs:
                    print(f"Recommendations for {test_email}: {recs}")
                    break
            else:
                print("No users with recommendations found in the tested sample.")

if __name__ == "__main__":
    main()
