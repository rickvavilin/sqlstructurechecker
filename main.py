__author__ = 'Aleksandr Vavilin'
from MySQLdb import connect, cursors
import tabulate
import copy
from datetime import datetime
import json
from json import JSONDecoder
from json import JSONEncoder
import argparse
import codecs


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

    def formatkeyschain(self, kc):
        return ' => '.join(kc)

    def formatdiff(self, diff):
        if diff['difftype'] == u'added' or diff['difftype'] == u'removed':
            return u'{} {}'.format(self.formatkeyschain(diff['keyschain']), diff['difftype'])
        else:
            return u'{} {} values: {}'.format(self.formatkeyschain(diff['keyschain']), diff['difftype'], u', '.join([unicode(v)[:80].strip() for v in diff['values']]))

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

def getitems(query, params, cur, keyfield):
    cur.execute(query, params)
    result = {}
    for item in cur.fetchall():
        result[item[keyfield]] = copy.deepcopy(item)
    cur.nextset()
    return result

def normalize(structure):
    return json.loads(json.dumps(structure, cls=DateTimeEncoder), cls=DateTimeDecoder)

def get_structure_from_database(host='localhost', user='root', passwd='2360087', database=None, **kwargs):
    db = connect(host=host, user=user, passwd=passwd, db='information_schema')
    cur = db.cursor(cursors.DictCursor)
    cur.execute("SET CHARSET 'utf8'")
    database_name = database
    cur.execute('select * from tables where table_schema=%s;', [database_name])
    tables_rows = cur.fetchall()
    cur.nextset()
    tables = {}
    for table in tables_rows:
        tables[table['TABLE_NAME']] = table
        table['COLUMNS'] = getitems('select * from columns where table_schema=%s and table_name=%s', [database_name, table['TABLE_NAME']], cur, 'COLUMN_NAME')
        table['CONSTRAINTS'] = getitems('select * from TABLE_CONSTRAINTS where table_schema=%s and table_name=%s', [database_name, table['TABLE_NAME']], cur, 'CONSTRAINT_NAME')
        table['TRIGGERS'] = getitems('select * from TRIGGERS where event_object_schema=%s and event_object_table=%s', [database_name, table['TABLE_NAME']], cur, 'TRIGGER_NAME')

    cur.execute('select * from routines where routine_schema=%s', [database_name])
    routines_rows = cur.fetchall()
    routines = {}
    for routine in routines_rows:
        routines[routine['ROUTINE_NAME']] = routine
        routine['PARAMETERS'] = getitems('select * from parameters where specific_schema=%s and specific_name=%s', [database_name, routine['ROUTINE_NAME']], cur, 'PARAMETER_NAME')
    cur.nextset()

    cur.execute('select * from views where table_schema=%s', [database_name])
    views_rows = cur.fetchall()
    views = {}
    for view in views_rows:
        views[view['TABLE_NAME']] = view
    cur.nextset()

    db.close()
    return normalize({u'TABLES': tables, u'ROUTINES': routines, u'VIEWS': views})


def dump_from_db_to_file(args):
    savestructure(args.output, get_structure_from_database(**vars(args)))

def compare_files(args):
    compare(loadstructure(args.input), loadstructure(args.input2))

def compare_file_vs_database(args):
    compare(loadstructure(args.input), get_structure_from_database(**vars(args)))

def compare(loaded_struct, parsed_struct):
    ignore = [[u'TABLES', u'CREATE_TIME'],
              [u'TABLES', u'DATA_FREE'],
              [u'TABLES', u'DATA_LENGTH'],
              [u'TABLES', u'INDEX_LENGTH'],
              [u'TABLES', u'TABLE_ROWS'],
              [u'TABLES', u'AUTO_INCREMENT'],
              [u'TABLES', u'AVG_ROW_LENGTH'],
              [u'TABLES', u'UPDATE_TIME'],
              [u'TABLES', u'COLUMNS', u'DATETIME_PRECISION'],
              [u'ROUTINES', u'CREATED'],
              [u'ROUTINES', u'LAST_ALTERED'],
    ]

    d = Differ()
    d.dictdiff(loaded_struct, parsed_struct,  0, [])
    for diff in d.diffs:
        if diff['keyschain'][0::2] not in ignore:
            print d.formatdiff(diff)
        if diff['keyschain'][0::2] == [u'ROUTINES', u'ROUTINE_DEFINITION']:
            f1 = codecs.open('old/'+diff['keyschain'][1]+'.sql', 'wb', encoding='utf-8')
            f1.write(unicode(diff['values'][0]))
            f1.close()
            f2 = codecs.open('new/'+diff['keyschain'][1]+'.sql', 'wb', encoding='utf-8')
            f2.write(unicode(diff['values'][1]))
            f2.close()


if __name__ == '__main__':
    commands = {"dump": dump_from_db_to_file,
                "compare": compare_file_vs_database,
                "filecompare": compare_files
                }

    parser = argparse.ArgumentParser(description='SQL structure checker')
    parser.add_argument("command", choices=list(commands))
    parser.add_argument("--host", "-H", metavar='HOST', default='localhost')
    parser.add_argument("--port", "-P", metavar='PORT', default=3306)
    parser.add_argument("--user", metavar='USER', default='root')
    parser.add_argument("--passwd", metavar='PASSWD', default='2360087')
    parser.add_argument("--database", "-D", metavar='DATABASE')
    parser.add_argument("--output", metavar='output filename')
    parser.add_argument("--input", metavar='input filename')
    parser.add_argument("--input2", metavar='input filename')


    args = parser.parse_args()
    commands[args.command](args)










