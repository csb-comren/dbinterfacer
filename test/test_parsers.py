import unittest
from dbinterfacer.uploaders import CidcoUploader, GeoJsonUploader, NmeaUploader
from .secret import local_url


class TestParsing(unittest.TestCase):
    def test_parse_json(self):
        f = open('test/data/geojson_small.json', 'rb')
        u = GeoJsonUploader(local_url, 'simple depth')
        u.parse_file(f)
        f.close()

        self.assertEqual(len(u.points), 610)

        self.assertAlmostEqual(float(u.max_lon), -64.295992)
        self.assertAlmostEqual(float(u.min_lon), -65.322957)
        self.assertAlmostEqual(float(u.max_lat), 44.170195)
        self.assertAlmostEqual(float(u.min_lat), 43.757667)

    def test_parse_nmea(self):
        f = open('test/data/NMEA.txt', 'rb')
        u = NmeaUploader(local_url, 'simple depth')
        u.parse_file(f)
        f.close()

        self.assertEqual(len(u.points), 980)

        self.assertAlmostEqual(float(u.max_lon), -53.1323188779000023487)
        self.assertAlmostEqual(float(u.min_lon), -53.134679757603332191706)
        self.assertAlmostEqual(float(u.max_lat), 47.3900974159199966209143)
        self.assertAlmostEqual(float(u.min_lat), 47.386470099866665094623)

    def test_parse_cidco(self):
        f = open('test/data/soundingExport.txt', 'rb')
        u = CidcoUploader(local_url, 'cidco processed')
        u.parse_file(f)
        f.close()

        self.assertEqual(len(u.points), 883)

        self.assertAlmostEqual(float(u.max_lon), -53.1323084)
        self.assertAlmostEqual(float(u.min_lon), -53.1346818)
        self.assertAlmostEqual(float(u.max_lat), 47.3900793)
        self.assertAlmostEqual(float(u.min_lat), 47.3864477)
