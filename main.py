__author__ = 'Aleksandr Vavilin'
from MySQLdb import connect, cursors
import tabulate
import copy
from datetime import datetime
import json
from json import JSONDecoder
from json import JSONEncoder



class DateTimeDecoder(json.JSONDecoder):

    def __init__(self, *args, **kargs):
        JSONDecoder.__init__(self, object_hook=self.dict_to_object,
                             *args, **kargs)

    def dict_to_object(self, d):
        if '__type__' not in d:
            return d

        type = d.pop('__type__')
        try:
            dateobj = datetime(**d)
            return dateobj
        except:
            d['__type__'] = type
            return d

class DateTimeEncoder(JSONEncoder):
    """ Instead of letting the default encoder convert datetime to string,
        convert datetime objects into a dict, which can be decoded by the
        DateTimeDecoder
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                '__type__' : 'datetime',
                'year' : obj.year,
                'month' : obj.month,
                'day' : obj.day,
                'hour' : obj.hour,
                'minute' : obj.minute,
                'second' : obj.second,
                'microsecond' : obj.microsecond,
            }
        else:
            return JSONEncoder.default(self, obj)

def printcur(cur):
    print tabulate.tabulate(cur.fetchall(), headers={d[0]:d[0] for d in cur.description})



db = connect(host='localhost', user='root', passwd='2360087', db='information_schema')
cur = db.cursor(cursors.DictCursor)

cur.execute("SET CHARSET 'utf8'")

cur.execute('select * from tables where table_schema=%s;', ['structure_test'])

tables_rows = cur.fetchall()

cur.nextset()

tables = {}

for table in tables_rows:
    cur.execute('select * from columns where table_name=%s order by ordinal_position asc',[table['TABLE_NAME']])
    column_rows = cur.fetchall()
    tables[table['TABLE_NAME']] = table
    table['COLUMNS'] = copy.deepcopy(column_rows)
    cur.nextset()


newrows = json.loads(json.dumps(tables, cls=DateTimeEncoder), cls=DateTimeDecoder)

#newrows2 = json.loads(json.dumps(tables, cls=DateTimeEncoder), cls=DateTimeDecoder)
#print json.dumps(tables, cls=DateTimeEncoder, indent=True)

f = open('./structure_test.json', 'r')
newrows2 = json.loads(f.read(), cls=DateTimeDecoder)
f.close()


for table in newrows.iterkeys():
    print table,':', newrows[table] == newrows2[table]
    if newrows[table] != newrows2[table]:
        for c in newrows[table].iterkeys():
            if (newrows[table][c] != newrows2[table][c]):
                if c=='COLUMNS':
                    for col in xrange(len(newrows[table][c])):
                        if (newrows[table][c][col] != newrows2[table][c][col]):
                            print 'column: ', col,  newrows[table][c][col],'!=', newrows2[table][c][col]

                else:
                    print newrows[table][c],'!=', newrows2[table][c]

#print newrows == newrows2





