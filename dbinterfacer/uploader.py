import psycopg2 as psyco        # pg driver

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


    def upload(self, dsn_string, user_id):
        """
        Makes a new batch and uploads all of the points
        :input: the dsn string, the user id
        """

        conn = psyco.connect(dsn=dsn_string)
        cur = conn.cursor()

        batch_id = self.insert_batch(cur, user_id)

        insert_points_string = self.generate_insert_string(batch_id)
        # TODO: psycopg2 has methods of optimizing running the same query a lot of times
        cur.executemany(insert_points_string, self.points);
        conn.commit();
        cur.close();
        conn.close();


    def insert_batch(self, cur, user_id, start_time=None):
        """
        Inserts a new batch into the database, returns batch_id
        :input: cursor, user_id, optional datetime representing the time
        the batch started
        """

        insert_batch_string = """
            INSERT INTO Batches (user_id, start_time) values (%s, %s) RETURNING id;
        """
        cur.execute(insert_batch_string, [user_id, start_time])
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

        if len(self.point_data_fields) > 0:
            insert_points += """
              if has_point_data = true then
                insert into point_data (pid, """

            for field in self.point_data_fields:
                insert_points += field + ", "

            insert_points = insert_points[:-2]
            insert_points += ") values (point_pk, "
            for field in self.point_data_fields:
                insert_points += "%("+ field + ")s, "

            insert_points = insert_points[:-2]
            insert_points += """);
              end if;
              """

        if len(self.collector_data_fields) > 0:
            insert_points += """
              if has_collector_data = true then
                insert into collector_data (pid, """

            for field in self.collector_data_fields:
                insert_points += field + ", "
            insert_points = insert_points[:-2]
            insert_points += ") values (point_pk, "
            for field in self.collector_data_fields:
                insert_points += "%("+ field + ")s, "
            insert_points = insert_points[:-2]
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
        Adds 2 boolean fields to the point signfying which tables
        the point will end up in
        """
        point['has_point_data'] = False
        for field in self.point_data_fields:
            if point[field] is not None:
                point['has_point_data'] = True
                break

        point['has_collector_data'] = False
        for field in self.collector_data_fields:
            if point[field] is not None:
                point['has_collector_data'] = True
                break
