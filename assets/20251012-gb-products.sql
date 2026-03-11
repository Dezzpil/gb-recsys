SELECT COUNT(*) FROM "Product" where type='game';
-- Всего игр 3284

SELECT COUNT(*) FROM (SELECT COUNT("productId") FROM "SimilarProduct" GROUP BY "productId") as g;
-- Всего игр с рекомендациями 2464

SELECT p.*,
       string_agg(distinct pg.name, ',') as genres,
       string_agg(distinct pp.name, ',') as platforms,
       string_agg(distinct sp.name, ',') as similar
FROM "Product" p
    LEFT JOIN "_ProductToProductGenre" ON p.id = "_ProductToProductGenre"."A"
    LEFT JOIN "ProductGenre" pg ON pg.id = "_ProductToProductGenre"."B"

    LEFT JOIN "_ProductToProductPlatform" ON p.id = "_ProductToProductPlatform"."A"
    LEFT JOIN "ProductPlatform" pp ON pp.id = "_ProductToProductPlatform"."B"

    LEFT JOIN "SimilarProduct" sp ON p.id = sp."productId"
GROUP BY p.id;
