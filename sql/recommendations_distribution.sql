-- Распределение количества рекомендаций для каждого email (которые есть в user_interactions)
-- для модели 'user_based_cf'

WITH email_recommendations_count AS (
    SELECT 
        r.email, 
        COUNT(*) as rec_count
    FROM 
        recommendations r
    WHERE 
        -- r.model_name = 'user_based_cf'
        r.model_name = 'similar_games'
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