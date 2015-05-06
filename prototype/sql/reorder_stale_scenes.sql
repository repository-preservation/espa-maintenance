update ordering_scene set status = 'submitted' where status = 'onorder' and order_id in (select id from ordering_order where order_date < DATE_SUB(CURDATE(), INTERVAL 15 DAY));
