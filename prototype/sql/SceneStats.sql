SELECT 
    SUM(scene_count) "Total Scene Count",
    SUM(CASE WHEN scene_count BETWEEN 1 AND 100 THEN scene_count END) "Order Size (1-100) Scene Count",
    (SUM(CASE WHEN scene_count BETWEEN 1 AND 100 THEN scene_count END) / SUM(scene_count)) * 100 "Percentage of Total Scenes",

    SUM(CASE WHEN scene_count BETWEEN 101 AND 1000 THEN scene_count END) "Order Size (101-1000) Scene Count",
    (SUM(CASE WHEN scene_count BETWEEN 101 AND 1000 THEN scene_count END) / SUM(scene_count)) * 100 "Percentage of Total Scenes",

    SUM(CASE WHEN scene_count > 1000 THEN scene_count END) "Order Size (1000+) Total Scenes",
    (SUM(CASE WHEN scene_count > 1000 THEN scene_count END) / SUM(scene_count)) * 100 "Percentage of Total Scenes"

FROM  (SELECT o.orderid AS orderid, count(*) AS scene_count FROM ordering_scene s, ordering_order o WHERE s.order_id = o.id GROUP BY o.orderid ORDER BY scene_count DESC) AS counts;
