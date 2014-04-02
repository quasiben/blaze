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

conn_string = "host='76.186.128.225' dbname='dev_test' user='quasiben' password='098poi'"
conn = psycopg2.extensions.connection(conn_string)
# cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cursor = conn.cursor()


sql = '''
 SELECT stocks.ticker, s
      tock_hist.c,
      stock_hist.o,
      stock_hist.date,
      FROM     stocks
      JOIN     stock_hist
          ON  stocks.sec_id = stock_hist.sec_id
'''

sql = 'select * from stocks limit 10;'
cursor.execute(sql)
data = cursor.fetchall()

psycopg2_blaze_types = {
    20: 'int64',
    25: 'string',
    1082: 'date',
    1114: 'datetime',
}

def psyco_to_dshape(type_code):
    return psycopg2_blaze_types[type_code]

def odbc_to_dshape(type_code):
    if str == type_code:
        return "string"
    elif int == type_code:
        return "int64"
    elif float == type_code:
        return "float64"
    elif datetime.datetime == type_code:
        return "date"

columns = []
types = []

for row in cursor.description:
    name, type_code, display_size, \
    internal_size, precision, \
    scale, null_ok = row
    columns.append(name)
    type_str = psyco_to_dshape(type_code)
    types.append(type_str)

row_size = str(len(data[0]))
row_count = str(len(data))

shape = '{}, {} '.format(row_size, row_count)



blz_cols = []
for c, t in zip(columns,types):
     blz_cols.append('{} : {}, '.format(c,t))

blz_cols = ' '.join(blz_cols)


dshape =' { '+ blz_cols+ ' } '
print dshape

print 'blaze array'
a = blaze.array(data,dshape=dshape)


# get list of tables;
sql = '''
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
'''

fname = "sample.h5"

with tb.open_file(fname, "w") as f:
    f.create_array(f.root, 'query_1', a.ddesc)