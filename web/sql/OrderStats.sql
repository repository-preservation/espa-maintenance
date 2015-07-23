SELECT 
    COUNT(orderid) "Total Order Count",
    SUM(CASE WHEN scene_count BETWEEN 1 AND 100 THEN 1 END) "Order with 1-100 Scenes",
    (SUM(CASE WHEN scene_count BETWEEN 1 AND 100 THEN 1 END) / COUNT(orderid)) * 100 "Percentage of Total Orders",
    
    SUM(CASE WHEN scene_count BETWEEN 101 AND 1000 THEN 1 END) "Orders With 101-1000 Scenes",
    (SUM(CASE WHEN scene_count BETWEEN 101 AND 1000 THEN 1 END) / COUNT(orderid)) * 100 "Percentage of Total Orders",
    
    SUM(CASE WHEN scene_count > 1000 THEN 1 END) "Orders with > 1000 Scenes",
    (SUM(CASE WHEN scene_count > 1000 THEN 1 END) / COUNT(orderid)) * 100 "Percentage of Total Orders"

FROM  (SELECT o.orderid AS orderid, count(*) AS scene_count FROM ordering_scene s, ordering_order o WHERE s.order_id = o.id GROUP BY o.orderid ORDER BY scene_count DESC) AS counts;
