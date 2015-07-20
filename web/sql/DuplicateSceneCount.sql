select count(name) Total,  count(distinct name) Uniques, (1- (count(distinct name)/count(name))) * 100 'Duplicate Percentage' from ordering_scene;
