from ..uploaders import Uploader
import ijson
from datetime import datetime

class GeoJsonUploader(Uploader):

    def parse_file(self, file):
        json_points = ijson.items(file, 'features.item')

        for jp in json_points:
            p = self.point_model.generate_point()
            time = datetime.strptime(jp['properties']['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
            p['time'] = time
            p['depth'] = jp['properties']['depth']
            p['longitude'] = jp['geometry']['coordinates'][0]
            p['latitude'] = jp['geometry']['coordinates'][1]
            self.add_point(p)

        super(GeoJsonUploader, self).parse_file(file)
