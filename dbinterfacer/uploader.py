import psycopg2 as psyco        # pg driver
import psycopg2.extras
class Uploader():
    """
    A generic class that has points and can upload them to the database
    Assumes the database has 3 tables: points (time, lat, lon), point_data and collector_data
    """

    def __init__(self, point_model, point_data_fields, collector_data_fields):
        """
        Needs the point model, and 2 arrays relating fields to tables
        """

        self.points = []
        self.point_model = point_model
        self.point_data_fields = point_data_fields
        self.collector_data_fields = collector_data_fields
        self.start_time = None


    def upload(self, dsn_string, file_ids):
        """
        Makes a new batch and uploads all of the points
        :input: the dsn string, a list of file_ids used in the batch
        """

        conn = psyco.connect(dsn=dsn_string)
        cur = conn.cursor()

        batch_id = self.insert_batch(cur)

        self.link_files_to_batch(cur, batch_id, file_ids)

        insert_points_string = self.generate_insert_string(batch_id)
        psycopg2.extras.execute_batch(cur, insert_points_string, self.points)
        conn.commit();
        cur.close();
        conn.close();


    def parse_file(self, file):
        """
        Abstract methods. Takes a file and makes corresponding points.
        """
        raise NotImplementedError("Please Implement this method")


    def insert_batch(self, cur):
        """
        Inserts a new batch into the database, returns batch_id
        :input: cursor
        """

        insert_batch_string = """
            INSERT INTO Batches (start_time) values (%s) RETURNING id;
        """
        cur.execute(insert_batch_string, [self.start_time])
        batch_id = cur.fetchone()[0]
        return batch_id


    def generate_insert_string(self, batch_id):
        """
        Makes a big sql string for inserting the fields to the correct tables
        and relating them to the point and batch
        :input: batch_id
        :output: a big SQL string
        """

        insert_points = """
            do $$
            declare
              point_pk bigint := 0;
              has_point_data boolean := %(has_point_data)s;
              has_collector_data boolean := %(has_collector_data)s;
            begin
              INSERT into points (time,latitude,longitude,batch_id) values (%(time)s, %(latitude)s, %(longitude)s, {batch});
              point_pk = currval('points_id_seq');
              """.format(batch=batch_id)

        if self.point_data_fields:
            insert_points += """
              if has_point_data = true then
                insert into point_data (pid, """

            insert_points += ", ".join(self.point_data_fields)
            insert_points += ") values (point_pk, "
            insert_points += ", ".join(map(lambda f: "%({})s".format(f), self.point_data_fields))
            insert_points += """);
              end if;
              """

        if self.collector_data_fields:
            insert_points += """
              if has_collector_data = true then
                insert into collector_data (pid, """
            insert_points += ", ".join(self.collector_data_fields)
            insert_points += ") values (point_pk, "
            insert_points += ", ".join(map(lambda f: "%({})s".format(f), self.collector_data_fields))
            insert_points += """);
              end if;"""

        insert_points += """
            end;
            $$language plpgsql;
        """
        return insert_points


    def add_point(self, point):
        """
        Stores the point, also validates and decides which tables the point goes in
        """
        if self.point_model.validate(point):
            self.determine_tables(point)
            self.points.append(point)


    def determine_tables(self, point):
        """
        Adds 2 boolean fields to the point signifying which tables
        the point will end up in
        """
        point['has_point_data'] = any(map(lambda f: point[f] is not None, self.point_data_fields))
        point['has_collector_data'] = any(map(lambda f: point[f] is not None, self.collector_data_fields))


    def link_files_to_batch(self, cur, batch_id, file_ids):
        """
        adds (batch_id, file_id) to batch_files for every file_id in file_ids
        """
        insert_tuples = map(lambda x: (batch_id, x), file_ids)
        insert_string = "INSERT INTO Batch_Files (batch_id, file_id) VALUES %s"
        psycopg2.extras.execute_values(cur, insert_string, insert_tuples)
