import psycopg2 as psyco


def query(dsn_string, sql_string, parameters=None):
    """
    Runs the query and returns an array of row-tuples
    :inputs: dsn_string and an sql string, optional sql parameters
    :outputs: an array of tuples
    """

    conn = psyco.connect(dsn=dsn_string)
    cur = conn.cursor()
    cur.execute(sql_string, parameters)

    result_rows = cur.fetchall()
    header = list(map(lambda col: col.name, cur.description))

    conn.commit();
    cur.close();
    conn.close();
    return result_rows, header


def get_select_string(outputs, tables, wheres):
    """
    Makes a full sql select statement (without a final ';')
    :inputs: 3 arrays
      * tables - tables used in the query (all possible joins are done)
      * wheres - an array of sql_boolean clauses, added to the WHERE part
      * outputs - which fields should be in the output (the SELECT part)
     :output: a big SQL string
    """

    output_s = ','.join(outputs)
    tables_s = ','.join(tables)

    # TODO: joins have not been updated
    tables.sort()
    all_joins = {
        'batch_files:batches' : 'batch_files.batch_id = batch.id',
        'batch_files:django.upload_file' : 'batch_files.file_id = django.upload_file.id',
        'batch_type_1:batches' : 'batch_type_1.batch_id = batch.id',
        'batch_type_fields:batch_types' : 'batch_type_fields.batch_type_id = batch_types.id',
        'batch_type_fields:lookup_field_types' : 'batch_type_fields.field_type = lookup_field_types.name',
        'django.upload_file:django.upload_upload' : 'django.upload_file.upload_id = django.upload_upload.id',
        'django.upload_upload:django.user_user' : 'django.upload_upload.user_id = django.user_user.id',
        'django.upload_upload:django.upload_upload_sensors' : 'django.upload_upload.id = django.upload_upload_sensors.upload_id',
        'django.upload_upload_sensors:django.user_sensor' : 'django.upload_upload_sensors.sensor_id = django.user_sensor.id',
        'django.upload_upload_sensors:django.user_sensor' : 'django.upload_upload_sensors.sensor_id = django.user_sensor.id',
    }

    joins = []
    for i, t1 in enumerate(tables):
        for t2 in tables[i + 1:]:
            combo = "{}:{}".format(t1, t2)
            if combo in all_joins:
                joins.append(all_joins[combo])
    joins_s = " and ".join(joins)

    other_wheres_s = ""
    if wheres:
        other_wheres_s =  " and ".join(wheres)

    where_s = " and ".join(filter(lambda x: x != '', [joins_s, other_wheres_s]))
    if where_s != '':
        where_s = "WHERE " + where_s

    select_s = """ SELECT %s FROM %s %s""" % (output_s, tables_s, where_s,)

    return select_s


def where_point_in_poly(poly_points):
    """
    a where bool, where geom is contained in the polygon made of the inputted points
    :input: an array of tuples [(lon, lat), (lon, lat)...]. THE LAST TUPLE SHOULD BE THE SAME
        AS THE FIRST
    :output: a where clause string
    """
    poly_points_string = map("{0[0]} {0[1]}".format, poly_points)
    linestring = "LINESTRING(" + ",".join(poly_points_string) + ")"
    where = "ST_Contains( ST_Polygon( ST_GeomFromText('" + linestring + "'), 4326), geom)"

    return where


def where_point_within_range(point, range):
    """
    a where bool, where geom is within the range (metres)
    :input: point - (lon, lat), range - positive number
    :output: a where clause string
    """
    radius_string = "ST_DWithin(ST_PointFromText('POINT({p[0]} {p[1]})', 4326) ::geography, geom, {r})"
    radius_string = radius_string.format(p=point, r=range)
    return radius_string


def get_batch_list(dsn_string, where_list=[]):
    select = get_select_string(
        [
            'batches.start_time',
            'batches.end_time',
            'batches.id',
        ],
        ['batches'],
        where_list)
    return query(dsn_string, select)


def get_batch_bbox(dsn_string, batch_id):
    select = get_select_string(
        ['ST_Xmin(bbox)', 'ST_Ymin(bbox)', 'ST_Xmax(bbox)', 'ST_Ymax(bbox)'],
        ['batches'],
        ['batches.id = %s'])
    return query(dsn_string, select, parameters=(batch_id, ))
