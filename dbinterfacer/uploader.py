from io import StringIO
from functools import reduce
import psycopg2 as psyco        # pg driver
import psycopg2.extras
from .helpers.exceptions import NoBatchTypeException
from .helpers.pointmodel import Point_Model


class Uploader():
    """
    The base class for other uploaders. The subclasses should essentially just implement the
    specific parser. This class initalizes the point model and interacts with the database.
    """


    def __init__(self, dsn_string, batch_type_name):
        """
        initalizes values and point_model
        :input:
            - dsn_string
            - the name of the batch_type the point_model is to be based off
                (should match the name of a type in the database)
        """

        self.dsn_string = dsn_string
        self.batch_type_name = batch_type_name
        self.points = []
        self.set_ref_table_and_fields()



    def upload(self, file_ids):
        """
        Makes a new batch and uploads all of the points. Also connects files to batchself.
        Returns the new batch_id
        :input: a list of file_ids used in the batch
        :output: int - id of batch
        """

        conn = psyco.connect(dsn=self.dsn_string)
        cur = conn.cursor()

        batch_id = self.insert_batch(cur)
        self.link_files_to_batch(cur, batch_id, file_ids)

        copy_file, header = self.make_csv(batch_id)
        cur.copy_from(copy_file, self.ref_table, columns=header)

        conn.commit();
        cur.close();
        conn.close();

        return batch_id


    def parse_file(self, file):
        """
        Takes a file and makes corresponding points and then gets the ranges of time and lat/lon.
        The parsing should be done in the subclasses and then super() should be used
        """

        self.set_time_range_and_bbox()


    def insert_batch(self, cur):
        """
        Inserts a new batch into the database, returns batch_id. Can only be run after parsing.
        :input: cursor
        """

        bbox_string = "ST_GeomFromText('POLYGON(({min_lon} {min_lat},{max_lon} {min_lat},{max_lon} {max_lat},{min_lon} {max_lat}, {min_lon} {min_lat}))', 4326)"
        bbox_string = bbox_string.format(min_lon=self.min_lon,max_lon=self.max_lon,min_lat=self.min_lat,max_lat=self.max_lat)

        insert_batch_string = """
            INSERT INTO Batches (start_time, end_time, batch_type_id, bbox)
            VALUES (%s, %s, %s, {}) RETURNING id;
        """.format(bbox_string)

        cur.execute(insert_batch_string, [self.start_time, self.end_time, self.batch_type_id])
        batch_id = cur.fetchone()[0]
        return batch_id


    def add_point(self, point):
        """
        Stores the point, also validates
        :input: a point dict
        """
        if self.point_model.validate(point):
            self.points.append(point)


    def link_files_to_batch(self, cur, batch_id, file_ids):
        """
        adds (batch_id, file_id) to batch_files for every file_id in file_ids
        :input: cursor, batch_id, iterable of file ids
        """
        insert_tuples = map(lambda x: (batch_id, x), file_ids)
        insert_string = "INSERT INTO Batch_Files (batch_id, file_id) VALUES %s"
        psycopg2.extras.execute_values(cur, insert_string, insert_tuples)


    def set_ref_table_and_fields(self):
        """
            using the batch_type_name set the ref_table and point_model based off the database
        """
        conn = psyco.connect(dsn=self.dsn_string)
        cur = conn.cursor()

        cur.execute('SELECT id, ref_table from batch_types where name = %s', (self.batch_type_name,))

        result = cur.fetchone()
        if result is None:
            raise NoBatchTypeException("There is no batch type with name '%s'" % self.batch_type_name)
        self.batch_type_id, self.ref_table = result

        cur.execute('SELECT field_name, field_type from batch_type_fields f, batch_types b where b.id = %s and b.id = f.batch_type_id', (self.batch_type_id,))
        fields = cur.fetchall()
        self.point_model = Point_Model(fields)

        conn.commit()
        cur.close()
        conn.close()


    def set_time_range_and_bbox(self):
        """
        Runs through all the points and finds the min and max of time, latitude and longitude
        Adds those vars to self. min_lon, max_lat, min_time, etc
        """
        def update_ranges(current, new):
            for key in current:
                val = current[key]
                if val is None:
                    current[key] = [new[key], new[key]]
                else:
                    current[key] = [min(current[key][0], new[key]), max(current[key][1], new[key])]
            return current

        ranges = { 'time':None, 'latitude':None, 'longitude':None }
        extremes = reduce(update_ranges, self.points, ranges)

        self.start_time, self.end_time = extremes['time']
        self.start_time, self.end_time = extremes['time']
        self.min_lat, self.max_lat = extremes['latitude']
        self.min_lon, self.max_lon = extremes['longitude']


    def make_csv(self, batch_id):
        """
        Makes a file-like object in tab-delimited CSV format without headers.
        Every point gets turned into a line.
        Returns a StringIO object and a list of strings representing the header.
        Any tabs in the values are replaced with spaces.
        :input: the batch_id
        :output: StringIO object in csv format, list of strings for header
        """
        fields = list(self.point_model.model)

        # remove all the fields that start with 'pr_' from points
        trimmed_points = map(lambda p: {f:p[f] for f in p if not f.startswith('pr_')}, self.points)

        copy_file = StringIO()
        for p in trimmed_points:
            # makes the line replacing any existing \t with spaces
            s = '\t'.join([str(p[f]).replace('\t', ' ') for f in fields] + [str(batch_id)])
            print(s, file=copy_file)

        copy_file.seek(0)

        header = fields + ['batch_id']

        return copy_file, header
