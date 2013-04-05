#This will select all orders that are in status 'ordered' but all the scenes are complete or unavailable

select id from ordering_order where id in 
    (select distinct order_id from ordering_scene where status in ('unavailable') group by order_id)
and id not in 
    (select distinct order_id from ordering_scene where status = 'onorder' group by order_id)
and status = 'ordered';


#This will update all said orders to the proper status and mark the completion date.
update ordering_order set status = 'complete', completion_date = CURDATE() where status = 'ordered' and id in (

    select tmpOrderTbl.tmpId from (
        
        select id as tmpId from ordering_order where id in 
            (select distinct order_id from ordering_scene where status = 'unavailable' group by order_id)
        and id not in 
            (select distinct order_id from ordering_scene where status = 'onorder' group by order_id)

    ) as tmpOrderTbl

);
