__author__ = 'Aleksandr Vavilin'
from MySQLdb import connect, cursors
import copy
from datetime import datetime
import json

default_ignore = [[u'TABLES', u'CREATE_TIME'],
              [u'TABLES', u'DATA_FREE'],
              [u'TABLES', u'DATA_LENGTH'],
              [u'TABLES', u'INDEX_LENGTH'],
              [u'TABLES', u'TABLE_ROWS'],
              [u'TABLES', u'AUTO_INCREMENT'],
              [u'TABLES', u'AVG_ROW_LENGTH'],
              [u'TABLES', u'UPDATE_TIME'],
              [u'TABLES', u'TABLE_SCHEMA'],
              [u'TABLES', u'COLUMNS', u'DATETIME_PRECISION'],
              [u'TABLES', u'TRIGGERS', u'TRIGGER_SCHEMA'],
              [u'TABLES', u'TRIGGERS', u'EVENT_OBJECT_SCHEMA'],
              [u'TABLES', u'COLUMNS', u'TABLE_SCHEMA'],
              [u'TABLES', u'CONSTRAINTS', u'TABLE_SCHEMA'],
              [u'TABLES', u'CONSTRAINTS', u'CONSTRAINT_SCHEMA'],
              [u'TABLES', u'REF_CONSTRAINTS', u'CONSTRAINT_SCHEMA'],
              [u'TABLES', u'REF_CONSTRAINTS', u'UNIQUE_CONSTRAINT_SCHEMA'],
              [u'ROUTINES', u'CREATED'],
              [u'ROUTINES', u'LAST_ALTERED'],
              [u'ROUTINES', u'PARAMETERS', u'SPECIFIC_SCHEMA'],
              [u'ROUTINES', u'ROUTINE_SCHEMA'],
    ]



class DateTimeDecoder(json.JSONDecoder):

    def __init__(self, *args, **kargs):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object,
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

class DateTimeEncoder(json.JSONEncoder):
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
            return json.JSONEncoder.default(self, obj)


class Differ():
    """
    Differ is class, that compute difference of two dicts or lists
    """
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
                    self.itemdiff(d1[key], d2[key], level+1, keyschain+[key])

    def listdiff(self, l1, l2, level, keyschain):
        for i in xrange(max(len(l1), len(l2))):
            if i < len(l1) and i < len(l2):
                if l1[i] != l2[i]:
                    self.itemdiff(l1[i], l2[i], level+1, keyschain+[i])
            elif i < len(l1):
                self.diffs.append(self.creatediff(None, None, keyschain+[i], 'removed'))
            elif i < len(l2):
                self.diffs.append(self.creatediff(None, None, keyschain+[i], 'added'))

    def structdiff(self, s1, s2):
        self.diffs = []
        self.dictdiff(s1, s2, 0, [])

    def filtereddiff(self, ignore):
        return filter(lambda x: x['keyschain'][0::2] not in ignore, self.diffs)


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
    """
    helper function for create dict of dicts from result
    of query. Keys for dict is values of keyfield column.
    Keyfield column must be unique in the result of query.
    """
    cur.execute(query, params)
    result = {}
    for item in cur.fetchall():
        result[item[keyfield]] = copy.deepcopy(item)
    cur.nextset()
    return result


def normalize(structure):
    return json.loads(json.dumps(structure, cls=DateTimeEncoder), cls=DateTimeDecoder)


def get_structure_from_database(host='localhost', user='root', passwd='2360087', port=3306, database=None, **kwargs):
    """
    :param host: hostname for database connection
    :param user: username for database connection
    :param passwd: password for database connection
    :param database: database name
    :param kwargs: for compatibility
    :return: structure of database metadata, include TABLES, VIEWS, CONSTRAINTS, ROUTINES
    """
    db = connect(host=host, user=user, passwd=passwd, db='information_schema', port=int(port))
    cur = db.cursor(cursors.DictCursor)
    cur.execute("SET CHARSET 'utf8'")
    database_name = database
    cur.execute('select * from tables where table_schema=%s;', [database_name])
    tables_rows = cur.fetchall()
    cur.nextset()
    tables = {}
    for table in tables_rows:
        print table['TABLE_NAME']
        tables[table['TABLE_NAME']] = table
        table['COLUMNS'] = getitems('select * from columns where table_schema=%s and table_name=%s', [database_name, table['TABLE_NAME']], cur, 'COLUMN_NAME')
        table['CONSTRAINTS'] = getitems('select * from TABLE_CONSTRAINTS where table_schema=%s and table_name=%s', [database_name, table['TABLE_NAME']], cur, 'CONSTRAINT_NAME')
        table['REF_CONSTRAINTS'] = getitems('select * from REFERENTIAL_CONSTRAINTS where constraint_schema=%s and table_name=%s', [database_name, table['TABLE_NAME']], cur, 'CONSTRAINT_NAME')
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
