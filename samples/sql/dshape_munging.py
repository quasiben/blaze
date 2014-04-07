import psycopg2
import pyodbc
import sqlite3
from itertools import izip

psycopg2_blaze_types = {
    20: 'int64',
    25: 'string',
    700: 'float64', #float
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

def cursor_to_dshape(cursor,data):

    if isinstance(cursor,sqlite3.Cursor):
        raise Exception('sqlite3 not supported')

    columns = []
    types = []

    #split between odbc and psycopg2
    if isinstance(cursor,psycopg2._psycopg.cursor):
        type_to_ndtype = psyco_to_dshape

    if isinstance(cursor,pyodbc.Cursor):
        type_to_ndtype = odbc_to_dshape


    for row in cursor.description:
        name, type_code, display_size, \
        internal_size, precision, \
        scale, null_ok = row
        columns.append(name)
        type_str = type_to_ndtype(type_code)
        types.append(type_str)

    row_size = str(len(data[0]))
    row_count = str(len(data))

    shape = '{}, {} '.format(row_size, row_count)

    # find maximum of all rows
    max_rows = [max(len(str(x)) for x in row) for row in izip(*data)]

    # find columns with type string
    str_cols = [i for i,t in enumerate(types) if t=='string']

    for col in str_cols:
        types[col] = "string["+str(max_rows[col])+",'utf8']"


    blz_cols = []
    for c, t in izip(columns,types):
         blz_cols.append('{} : {}, '.format(c,t))

    blz_cols = ' '.join(blz_cols)


    dshape =' { '+ blz_cols+ ' } '

    return dshape
