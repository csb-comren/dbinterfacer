#!/Users/jaykaron/csb-comren/ENV/bin/python

from dbinterfacer.uploaders.nmea import NMEA_Uploader
from dbinterfacer.helpers.pointmodel import Point_Model

from secret import local_url, comren_url

u = NMEA_Uploader()
f = open('test/NMEA.txt', 'rb')
p = u.point_model.generate_point()
# print(p)
# u.determine_tables(p)
# print(p)
# u.parse_file(f)
# u.upload(local_url, 6, [4,10])
