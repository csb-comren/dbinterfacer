import unittest
from dbinterfacer.uploaders import GeoJSON_Uploader, NMEA_Uploader
from .secret import local_url


class TestParsing(unittest.TestCase):
    def test_parse_json(self):
        f = open('test/data/geojson_small.json', 'rb')
        u = GeoJSON_Uploader(local_url, 'simple depth')
        u.parse_file(f)
        f.close()

        self.assertEqual(len(u.points), 20887)

        self.assertAlmostEqual(float(u.max_lon), -64.295992)
        self.assertAlmostEqual(float(u.min_lon), -65.329237)
        self.assertAlmostEqual(float(u.max_lat), 44.170195)
        self.assertAlmostEqual(float(u.min_lat), 43.626069)

    def test_parse_nmea(self):
        f = open('test/data/NMEA.txt', 'rb')
        u = NMEA_Uploader(local_url, 'simple depth')
        u.parse_file(f)
        f.close()

        self.assertEqual(len(u.points), 980)

        self.assertAlmostEqual(float(u.max_lon), -53.1323188779000023487)
        self.assertAlmostEqual(float(u.min_lon), -53.134679757603332191706)
        self.assertAlmostEqual(float(u.max_lat), 47.3900974159199966209143)
        self.assertAlmostEqual(float(u.min_lat), 47.386470099866665094623)
