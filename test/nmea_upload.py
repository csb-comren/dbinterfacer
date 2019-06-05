from dbinterfacer.uploaders import NMEA_Uploader
from dbinterfacer.helpers.pointmodel import Point_Model

from secret import local_url, comren_url

f = open('test/data/NMEA.txt', 'rb')
# p = u.point_model.generate_point()
# print(p)
# u.determine_tables(p)
# print(p)

# u.upload(local_url, [1])


u = NMEA_Uploader(local_url, 'simple depth')
u.parse_file(f)
u.upload([])
