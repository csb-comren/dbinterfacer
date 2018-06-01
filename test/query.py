#!/Users/jaykaron/csb-comren/ENV/bin/python

import dbinterfacer.query as qu
from secret import local_url, comren_url

print(qu.get_all_points(local_url))
