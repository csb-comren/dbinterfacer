#!/Users/jaykaron/csb-comren/ENV/bin/python

from dbinterfacer.uploaders.geojson import GeoJSON_Uploader
from dbinterfacer.helpers.pointmodel import Point_Model
from secret import local_url, comren_url

f = open('test/geojson_big.json', 'rb')
u = GeoJSON_Uploader()
u.parse_file(f)

print(len(u.points))

# u.upload(local_url, [17])
