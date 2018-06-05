import psycopg2 as psyco

def query(dsn_string, sql_string):
    conn = psyco.connect(dsn=dsn_string)
    cur = conn.cursor()
    cur.execute(sql_string)
    result_rows = cur.fetchall()

    conn.commit();
    cur.close();
    conn.close();
    return result_rows

def get_all_points(dsn_string):
    get_all_string = """
    SELECT jsonb_build_object(
        'type', 'Feature',
        'id', id,
        'depth', depth,
        'geometry', ST_AsGeoJSON(geom)::jsonb
        ) as feature
        FROM (select p.id, p.geom, pd.depth from points p, point_data pd where p.id = pd.pid) as inputs;
    """

    rows = query(dsn_string, get_all_string)
    results = map(lambda r: r[0], rows)
    return results
