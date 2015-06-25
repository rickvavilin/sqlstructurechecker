__author__ = 'Aleksandr Vavilin'
from MySQLdb import connect, cursors
import tabulate
import copy
from datetime import datetime
import json
from json import JSONDecoder
from json import JSONEncoder
import prettyprint



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

def getindent(level):
    return ' '*level*4

class Differ():
    def __init__(self):
        self.diffs = []

    def creatediff(self, i1, i2, keyschain, difftype):
        return {'keyschain': keyschain, 'difftype': difftype, 'values': [i1, i2]}

    def itemdiff(self, i1, i2, level, keyschain):
        if isinstance(i1, dict) and isinstance(i2, dict):
            self.dictdiff(i1, i2, level, keyschain)
        elif isinstance(i1, list) and isinstance(i2, list):
            self.listdiff(i1, i2, level, keyschain)
        else:
            #print getindent(level), keyschain, i1, i2
            self.diffs.append(self.creatediff(i1, i2, keyschain, 'differ'))

    def dictdiff(self, d1, d2, level, keyschain):
        for key in d1.iterkeys():
            if key not in d2:
                self.diffs.append(self.creatediff(None, None, keyschain+[key], 'removed'))
        for key in d2.iterkeys():
            if key not in d1:
                self.diffs.append(self.creatediff(None, None, keyschain+[key], 'added'))

        for key in d1.iterkeys():
            if key in d2:
                if d1[key]!=d2[key]:
                    #print getindent(level), 'differ: ', key, ':'
                    self.itemdiff(d1[key], d2[key], level+1, keyschain+[key])


    def listdiff(self, l1, l2, level, keyschain):
        for i in xrange(max(len(l1), len(l2))):
            if i < len(l1) and i < len(l2):
                if l1[i] != l2[i]:
                    #print getindent(level), 'differ: ', i, ':'
                    self.itemdiff(l1[i], l2[i], level+1, keyschain+[i])
            elif i < len(l1):
                self.diffs.append(self.creatediff(None, None, keyschain+[i], 'removed'))
            elif i < len(l2):
                self.diffs.append(self.creatediff(None, None, keyschain+[added], 'removed'))



def savestructure(filename, structure):
    f = open(filename, 'w')
    f.write(json.dumps(structure, indent=True, cls=DateTimeEncoder))
    f.close()


def loadstructure(filename):
    f = open(filename, 'r')
    structure = json.loads(f.read(), cls=DateTimeDecoder)
    f.close()
    return structure

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
    columns = {}
    for column in column_rows:
        columns[column['COLUMN_NAME']] = copy.deepcopy(column)
    table['COLUMNS'] = columns
    cur.nextset()


newrows = json.loads(json.dumps(tables, cls=DateTimeEncoder), cls=DateTimeDecoder)

#print json.dumps(tables, cls=DateTimeEncoder, indent=True)

#savestructure('./structure_test.json', tables)
#newrows2 = json.loads(json.dumps(tables, cls=DateTimeEncoder), cls=DateTimeDecoder)

#f = open('./structure_test.json', 'r')
#newrows2 = json.loads(f.read(), cls=DateTimeDecoder)
#f.close()

newrows2 = loadstructure('./structure_test.json')


d = Differ()

d.dictdiff(newrows2, newrows,  0, [u'TABLES'])

prettyprint.pp(d.diffs)






