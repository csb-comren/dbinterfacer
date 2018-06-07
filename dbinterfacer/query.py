import psycopg2 as psyco

def query(dsn_string, sql_string):
    """
    Runs the query and returns an array of row-tuples
    :inputs: dsn_string and an sql string
    :outputs: an array of tuples
    """

    conn = psyco.connect(dsn=dsn_string)
    cur = conn.cursor()
    cur.execute(sql_string)
    result_rows = cur.fetchall()

    conn.commit();
    cur.close();
    conn.close();
    return result_rows


def get_depth_points_json(dsn_string, where=[]):
    """
    returns an array of geojson depth points filtered by where clause
    :input: dsn_string, optional array of where strings
    :ouput: a map of an array of the geojson points
    """
    select = get_select_string(['points', 'point_data'], where, ['points.id','points.geom', 'point_data.depth'])

    get_all_string = """
    SELECT jsonb_build_object(
        'type', 'Feature',
        'id', id,
        'depth', depth,
        'geometry', ST_AsGeoJSON(geom)::jsonb
        ) as feature
        FROM ({}) as inputs;
    """.format(select)

    rows = query(dsn_string, get_all_string)
    results = map(lambda r: r[0], rows)
    return results


def get_select_string(tables, wheres, outputs):
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

    tables.sort()
    all_joins = {
        'batch:points' : 'batch.id = points.batch_id',
        'collector_data:points' : 'points.id = collector_data.pid',
        'point_data:points' : 'points.id = point_data.pid',
    }

    joins = []
    for i, t1 in enumerate(tables):
        for t2 in tables[i + 1:]:
            combo = "{}:{}".format(t1, t2)
            if combo in all_joins:
                joins.append(all_joins[combo])
    joins_s = " and ".join(joins)

    if wheres:
        where_s = " and ({})".format(" and ".join(wheres))
    else:
        where_s = ""

    select_s = """ SELECT %s from %s WHERE %s%s""" % (output_s, tables_s, joins_s, where_s,)

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
    radius_string = "ST_DWithin(ST_PointFromText('POINT({p[0]} {p[1]})', 4326) ::geography, points.geom, {r})"
    radius_string = radius_string.format(p=point, r=range)
    return radius_string
