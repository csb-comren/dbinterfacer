#!/Users/jaykaron/csb-comren/ENV/bin/python

import dbinterfacer.query as qu
from secret import local_url, comren_url

points = [(-50, 45),(-55, 45),(-52.5, 50), (-50, 45)]
where_in_poly = qu.where_point_in_poly(points)
# select = qu.get_select_string(['points', 'point_data'], [where_in_poly, 'depth > 15'], ['latitude', 'depth'])
radius = qu.where_point_within_range((-53.13233, 47.40085), 2000)
select = qu.get_select_string(['depth', 'latitude', 'longitude', 'geom'], ['batch_type_1'], [where_in_poly])
print(select)

json_q = qu.get_json([("'type'", "'Feature'"), ("'depth'", 'depth'), ("'geometry'", "geom")], select)
print(json_q)
# results = qu.query(local_url, json_q)
# print(results)
# print(len(results))
# qu.get_depth_points_json("a")
