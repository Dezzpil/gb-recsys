-- Распределение количества рекомендаций для каждого email (которые есть в user_interactions)
-- сразу от двух моделей: 'user_based_cf' и 'similar_games'

delete from recommendations where merge_log_id is null;

WITH email_recommendations_count AS (
    SELECT 
        r.email, 
        COUNT(DISTINCT r.product_id) as rec_count
    FROM 
        recommendations r
    WHERE 
        r.model_name IN ('user_based_cf', 'similar_games')
        AND r.email IN (SELECT DISTINCT email FROM user_interactions)
    GROUP BY 
        r.email
)
SELECT 
    rec_count as recommendations_per_user,
    COUNT(*) as users_count
FROM 
    email_recommendations_count
GROUP BY 
    rec_count
ORDER BY 
    rec_count;