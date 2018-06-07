#!/Users/jaykaron/csb-comren/ENV/bin/python

import dbinterfacer.query as qu
from secret import local_url, comren_url

# points = [(-50, 45),(-55, 45),(-52.5, 50), (-50, 45)]
# where_in_poly = qu.where_point_in_poly_string(local_url, points)
# select = qu.get_select_string(['points', 'point_data'], [where_in_poly, 'depth > 15'], ['latitude', 'depth'])
# radius = qu.where_point_within_range((-53.13233, 47.40085), 2000)
# select = qu.get_select_string(['points', 'point_data'], [radius], ['depth', 'latitude'])
# print(select)
print(len(list(qu.get_depth_points_json(local_url, ['depth > -1']))))
