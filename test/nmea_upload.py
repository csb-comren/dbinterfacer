#!/Users/jaykaron/csb-comren/ENV/bin/python

from dbinterfacer.uploaders.nmea import NMEA_Uploader
from dbinterfacer.helpers.pointmodel import Point_Model

from secret import local_url, comren_url

pm = Point_Model()

u = NMEA_Uploader()
f = open('test/NMEA.txt', 'rb')

u.parse_file(f)
u.upload(local_url, 6, [4,10])