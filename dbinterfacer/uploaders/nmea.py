from ..uploaders import Uploader
from ..helpers.pointmodel import Point_Model

import pynmea2
from datetime import datetime
from decimal import Decimal


class NMEA_Uploader(Uploader):
    """
    Class for upload NMEA files. Uploads only: time, lat, lon, depth
    """

    def parse_file(self, file):
        input = file.__iter__()
        streamreader = pynmea2.NMEAStreamReader()

        depth_queue = []
        self.prev_gps, self.next_gps = (None, None)


        all_sentences = ("$GPGGA",
                        "$GPGSA",
                        "$GPRMC",
                        "$GPVTG",
                        "$PADBT",
                        "$PTNTHPR",
                        "$STATUS",
                        '$SDDBT')
        accepted_sentences = ("$GPRMC", "$PADBT",'$SDDBT')

        for l in input:
            l = l.decode('utf-8')
            if l == '\n':
                continue
            try:
                time, data = l.split(" ")
            except Exception as e:
                continue
            if data.startswith(accepted_sentences):
                for msg in streamreader.next(data):
                    if isinstance(msg, pynmea2.RMC):
                        self.update_gps(msg)
                        # try processing depth msgs
                        if len(depth_queue) > 0:
                            ready_depth_points = self.make_depth_points(depth_queue)
                            for p in ready_depth_points:
                                self.add_point(p)

                    if isinstance(msg, pynmea2.types.proprietary.adb.ADBT) or isinstance(msg, pynmea2.types.talker.DBT):
                        depth_queue.append((msg, time))

        super(NMEA_Uploader, self).parse_file(file)

    def rmc_to_point(self, m):

        point = self.point_model.generate_point()
        point['time'] = m.time
        point['latitude'] = m.lat_r
        point['longitude'] = m.lon_r
        point['pr_speed'] = m.spd_over_grnd
        point['pr_course'] = m.true_course

        return point


    def update_gps(self, msg):
        msg.time = datetime.combine(msg.datestamp, msg.timestamp)
        msg.lat_r = NMEA_Uploader.nmea_l_to_dec(msg.lat, msg.lat_dir)
        msg.lon_r = NMEA_Uploader.nmea_l_to_dec(msg.lon, msg.lon_dir)
        self.prev_gps = self.next_gps
        self.next_gps = msg


    def make_depth_points(self, depth_queue):
        # try determining locations of depth points
        ready_messages = self.calculate_locations(depth_queue)

        ready_points = []
        for m in ready_messages:
            point = self.point_model.generate_point()
            point['time'] = m.time
            point['latitude'] = m.lat_r
            point['longitude'] = m.lon_r
            point['depth'] = m.depth_meters
            ready_points.append(point)

        return ready_points


    def calculate_locations(self, depth_queue):
        if self.prev_gps == None or self.next_gps == None:
            return []

        time_delta = self.next_gps.time - self.prev_gps.time
        lat_delta = self.next_gps.lat_r - self.prev_gps.lat_r
        lon_delta = self.next_gps.lon_r - self.prev_gps.lon_r

        results = []
        for i in range(len(depth_queue)):
            (msg, time_string) = depth_queue.pop(0)
            time_obj = datetime.strptime(time_string, "%H:%M:%S.%f").time()
            time = datetime.combine(self.prev_gps.datestamp, time_obj)
            if time > self.next_gps.time:
                depth_queue.append((msg, time_string))
                continue
            time_d = time - self.prev_gps.time
            time_ratio = time_d / time_delta
            msg.time = time
            msg.lat_r = lat_delta * Decimal(time_ratio) + self.prev_gps.lat_r
            msg.lon_r = lon_delta * Decimal(time_ratio) + self.prev_gps.lon_r
            results.append(msg)

        return results


    def nmea_l_to_dec(nmea_real, compass):
        """
        Converts a string of 'dddmm.mmmmmmm' and 'D' (N,E,S,W)
        to a single Decimal of dd.dddddd
        (floats were messing up the sql string)
        :param nmea_real: the string float
        :param compass: a char one of [N,E,S,W]
        :returns: a Decimal
        """
        nmea_real = float(nmea_real)
        degrees = int(nmea_real / 100)
        minutes = nmea_real % 100
        result = degrees + minutes / 60.0
        if compass == 'W' or compass == 'S':
            result = -1 * result
        return Decimal(result)
