from ..uploader import Uploader
from ..helpers.pointmodel import Point_Model

import ijson
from decimal import Decimal
from datetime import datetime

class GeoJSON_Uploader(Uploader):

    def parse_file(self, file):
        input = file.__iter__()
        json_points = ijson.items(file, 'features.item')

        for jp in json_points:
            p = self.point_model.generate_point()
            time = datetime.strptime(jp['properties']['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
            if self.start_time is None:
                self.start_time = time
            p['time'] = time
            p['depth'] = jp['properties']['depth']
            p['longitude'] = jp['geometry']['coordinates'][0]
            p['latitude'] = jp['geometry']['coordinates'][1]
            self.add_point(p)
