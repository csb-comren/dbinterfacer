from ..uploaders import Uploader

from decimal import Decimal
from datetime import datetime


class CidcoUploader(Uploader):

    def parse_file(self, file):
        # skip 2 header lines
        next(file, None)
        next(file, None)

        for row in file:
            # decode from byte to string and split
            entries = row.decode('utf-8').split(';')
            p = self.point_model.generate_point()
            p['time'] = datetime.strptime(entries[0], '%Y/%m/%d %H:%M:%S.%f')
            p['latitude'] = Decimal(entries[1])
            p['longitude'] = Decimal(entries[2])
            p['depth'] = Decimal(entries[3])
            p['northing'] = Decimal(entries[4])
            p['easting'] = Decimal(entries[5])

            self.add_point(p)

        super(CidcoUploader, self).parse_file(file)

