import psycopg2 as psyco

def query(dsn_string, sql_string):
    conn = psyco.connect(dsn=dsn_string)
    cur = conn.cursor()
    cur.execute(sql_string)
    if cur.rowcount > 0:
        results = cur.fetchall()
    else:
        results = []
    conn.commit();
    cur.close();
    conn.close();
    return results

def get_all_points(dsn_string):
    get_all_string = """
    SELECT jsonb_build_object(
        'type', 'Feature',
        'id', id,
        'geometry', ST_AsGeoJSON(geom)::jsonb
        ) as feature
        FROM (select p.id, p.geom, pd.depth from points p, point_data pd where p.id = pd.pid) as inputs;
    """

    return query(dsn_string, get_all_string)
