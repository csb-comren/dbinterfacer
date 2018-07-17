#!/Users/jaykaron/csb-comren/ENV/bin/python

from dbinterfacer.uploaders.geojson import GeoJSON_Uploader
from dbinterfacer.helpers.pointmodel import Point_Model
from secret import local_url, comren_url

f = open('test/data/geojson_small.json', 'rb')
u = GeoJSON_Uploader(local_url, 'simple depth')
u.parse_file(f)

print(len(u.points))

u.upload([])
