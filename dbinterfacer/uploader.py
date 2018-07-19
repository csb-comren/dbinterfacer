from io import StringIO
from functools import reduce
import psycopg2 as psyco        # pg driver
import psycopg2.extras
from .helpers.exceptions import NoBatchTypeException
from .helpers.pointmodel import Point_Model


class Uploader():
    """
    A generic class that has points and can upload them to the database
    """


    def __init__(self, dsn_string, batch_type_name):
        """
        initalizes values and point_model
        :input:
            - dsn_string
            - the name of the batch_type the point_model is to be based off
        """

        self.dsn_string = dsn_string
        self.batch_type_name = batch_type_name
        self.points = []
        self.start_time = None                  # the time when the surveyed started, should be set when the file is parsed
        self.set_ref_table_and_fields()



    def upload(self, file_ids):
        """
        Makes a new batch and uploads all of the points
        :input: a list of file_ids used in the batch
        Returns the new batch_id
        """

        conn = psyco.connect(dsn=self.dsn_string)
        cur = conn.cursor()

        batch_id = self.insert_batch(cur)
        self.link_files_to_batch(cur, batch_id, file_ids)

        fields = list(self.point_model.model)

        # remove all the fields that start with 'pr_' from points
        trimmed_points = map(lambda p: {f:p[f] for f in p if not f.startswith('pr_')}, self.points)

        # make a tab-delimited CSV for the copy statement
        # TODO: string escaping stuff
        copy_file = StringIO()
        for p in trimmed_points:
            s = '\t'.join([str(p[f]) for f in fields] + [str(batch_id)])
            print(s, file=copy_file)

        copy_file.seek(0)
        header = fields + ['batch_id']
        cur.copy_from(copy_file, self.ref_table, columns=header)

        conn.commit();
        cur.close();
        conn.close();

        return batch_id


    def parse_file(self, file):
        """
        Takes a file and makes corresponding points and then gets the ranges of time and lat/lon.
        The parsing should be done in the subclasses and then super() should be user
        """

        self.set_time_range_and_bbox()
        # raise NotImplementedError("This is not implemented in the base class")


    def insert_batch(self, cur):
        """
        Inserts a new batch into the database, returns batch_id
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
        """
        if self.point_model.validate(point):
            self.points.append(point)


    def link_files_to_batch(self, cur, batch_id, file_ids):
        """
        adds (batch_id, file_id) to batch_files for every file_id in file_ids
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
            raise NoBatchTypeException("There is no batch type with name %s" % self.batch_type_name)
        self.batch_type_id, self.ref_table = result

        cur.execute('SELECT field_name, field_type from batch_type_fields f, batch_types b where b.id = %s and b.id = f.batch_type_id', (self.batch_type_id,))
        fields = cur.fetchall()
        self.point_model = Point_Model(fields)

        conn.commit()
        cur.close()
        conn.close()


    def set_time_range_and_bbox(self):
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
