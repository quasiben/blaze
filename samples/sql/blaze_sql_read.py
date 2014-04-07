import sqlite3
from datashape import dshape
import blaze
from blaze.io.sql import sql_table
import datetime
import pyodbc
import psycopg2
import psycopg2.extensions
import psycopg2.extras
psycopg2._psycopg.connection
from dateutil import parser
from datetime import date

from dshape_munging import cursor_to_dshape

conn_string = "host='76.186.128.225' dbname='dev_test' user='quasiben' password='098poi'"
conn = psycopg2.extensions.connection(conn_string)
# cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cursor = conn.cursor()

# get list of tables;
sql = '''
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
'''

sql = '''
 SELECT stocks.ticker,
      stock_hist.c,
      stock_hist.o,
      stocks.sec_id
      FROM     stocks
      JOIN     stock_hist
          ON  stocks.sec_id = stock_hist.sec_id
'''

# sql = 'select sec_id, ticker  from stocks limit 10;'
cursor.execute(sql)
data = cursor.fetchall()


dshape = cursor_to_dshape(cursor,data)
print dshape

fname = "sample.h5"

hdf5ddesc = blaze.HDF5_DDesc(path=fname, datapath='/table', mode='w',allow_copy=True)

a = blaze.array(data,dshape=dshape,ddesc=hdf5ddesc)



# nd.as_numpy(b.ucast("string[6,'utf8']"),allow_copy=True)
#
# with tb.open_file(fname, "w") as f:
#     f.create_array(f.root, 'query_1', a.ddesc)