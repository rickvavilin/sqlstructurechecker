__author__ = 'Aleksandr Vavilin'
from MySQLdb import connect, cursors
import copy
from datetime import datetime
import json
import codecs

default_ignore = [[u'TABLES', u'CREATE_TIME'],
              [u'TABLES', u'CREATE'],
              [u'TABLES', u'DATA_FREE'],
              [u'TABLES', u'DATA_LENGTH'],
              [u'TABLES', u'INDEX_LENGTH'],
              [u'TABLES', u'TABLE_ROWS'],
              [u'TABLES', u'AUTO_INCREMENT'],
              [u'TABLES', u'AVG_ROW_LENGTH'],
              [u'TABLES', u'UPDATE_TIME'],
              [u'TABLES', u'TABLE_SCHEMA'],
              [u'TABLES', u'COLUMNS', u'DATETIME_PRECISION'],
              [u'TABLES', u'COLUMNS', u'ORDINAL_POSITION'],
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
              [u'TABLES', u'INDEXES', u'Cardinality'],
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
        return ' => '.join([unicode(k) for k in kc])

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

def getindexes(query, params, cur, keyfield):
    cur.execute(query, params)
    result = {}
    for item in cur.fetchall():
        idx = copy.deepcopy(item)

        if item[keyfield] not in result:
            result[item[keyfield]] = idx
            result[item[keyfield]]['COLUMNS'] = [idx['Column_name']]
            del result[item[keyfield]]['Column_name']
        else:
            result[item[keyfield]]['COLUMNS'].append(idx['Column_name'])

    cur.nextset()
    return result


def dump_routine(cur, database, proctype, proc):
    #print """SHOW CREATE {} {}.{};""".format(proctype, database,  proc)
    cur.execute("""SHOW CREATE {} {}.{};""".format(proctype, database,  proc))
    f = 'Create Procedure'
    if proctype=='FUNCTION':
        f = 'Create Function'
    pdef = "DROP {} IF EXISTS {};\nDELIMITER $$\n".format(proctype, proc) + cur.fetchall()[0][f] + "\n$$\nDELIMITER ;\n"
    return pdef

def dump_trigger(cur, database, trigger):
    #print """SHOW CREATE {} {}.{};""".format(proctype, database,  proc)
    cur.execute("""SHOW CREATE TRIGGER {}.{};""".format(database,  trigger))
    f = 'SQL Original Statement'
    pdef = "DROP TRIGGER IF EXISTS {};\nDELIMITER $$\n".format(trigger) + cur.fetchall()[0][f] + "\n$$\nDELIMITER ;\n"
    return pdef

def dump_table(cur, database, table):
    #print """SHOW CREATE {} {}.{};""".format(proctype, database,  proc)
    cur.execute("""SHOW CREATE TABLE {}.{};""".format(database,  table))
    f = 'Create Table'
    pdef = cur.fetchall()[0][f]
    return pdef


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
        tables[table['TABLE_NAME']] = table
        table['CREATE'] = dump_table(cur, database_name, table['TABLE_NAME'])
        table['COLUMNS'] = getitems('select * from columns where table_schema=%s and table_name=%s', [database_name, table['TABLE_NAME']], cur, 'COLUMN_NAME')

        table['CONSTRAINTS'] = getitems('select * from TABLE_CONSTRAINTS where table_schema=%s and table_name=%s', [database_name, table['TABLE_NAME']], cur, 'CONSTRAINT_NAME')
        table['REF_CONSTRAINTS'] = getitems('select * from REFERENTIAL_CONSTRAINTS where constraint_schema=%s and table_name=%s', [database_name, table['TABLE_NAME']], cur, 'CONSTRAINT_NAME')
        table['TRIGGERS'] = getitems('select * from TRIGGERS where event_object_schema=%s and event_object_table=%s', [database_name, table['TABLE_NAME']], cur, 'TRIGGER_NAME')
        table['FOREIGN_KEYS'] = getitems('select CONSTRAINT_NAME , column_name,  referenced_table_name, referenced_column_name from key_column_usage where referenced_table_name is not null and table_schema = %s and table_name = %s', [database_name, table['TABLE_NAME']], cur, 'CONSTRAINT_NAME');
        table['INDEXES'] = getindexes('SHOW INDEX FROM {}.{}'.format(database_name, table['TABLE_NAME']),[], cur, 'Key_name')
        for trigger in table['TRIGGERS']:
            table['TRIGGERS'][trigger]['CREATE'] = dump_trigger(cur, database_name, trigger)
        for column in table['COLUMNS']:
            if table['COLUMNS'][column]['ORDINAL_POSITION']>1:
                for c in table['COLUMNS']:
                    if table['COLUMNS'][c]['ORDINAL_POSITION']==table['COLUMNS'][column]['ORDINAL_POSITION']-1:
                        table['COLUMNS'][column]['PREVIOUS'] = c
            else:
                table['COLUMNS'][column]['PREVIOUS'] = None




        cur.execute('select * from routines where routine_schema=%s', [database_name])
    routines_rows = cur.fetchall()
    routines = {}
    for routine in routines_rows:
        routines[routine['ROUTINE_NAME']] = routine
        routine['PARAMETERS'] = getitems('select * from parameters where specific_schema=%s and specific_name=%s', [database_name, routine['ROUTINE_NAME']], cur, 'PARAMETER_NAME')
        routine['CREATE'] = dump_routine(cur,database_name,routine['ROUTINE_TYPE'], routine['ROUTINE_NAME'])
    cur.nextset()

    cur.execute('select * from views where table_schema=%s', [database_name])
    views_rows = cur.fetchall()
    views = {}
    for view in views_rows:
        views[view['TABLE_NAME']] = view
    cur.nextset()

    db.close()
    return normalize({u'TABLES': tables, u'ROUTINES': routines, u'VIEWS': views})


class Comparer(object):

    def __init__(self, loaded_struct, parsed_struct, ignore=default_ignore):
        self.loaded_struct = loaded_struct
        self.parsed_struct = parsed_struct
        self.ignore = ignore
        self.all_alters = []
        self.formatted_diff = []

    def compare(self):
        alters = []
        modify_column_alters = {}
        idx_alters = []
        proc_alters = []
        d = Differ()
        d.structdiff(self.loaded_struct, self.parsed_struct)

        filtered_diffs = d.filtereddiff(self.ignore)
        #if len(filtered_diffs) == 0:
        #    print 'No differences found'

        for diff in filtered_diffs:
            if diff['keyschain'][0::2][:2] == [u'TABLES']:
                if diff['difftype']==u'removed':
                    a = 'DROP TABLE {};'.format(diff['keyschain'][1])
                    if a not in alters:
                        alters.append(a)
                if diff['difftype']==u'added':
                    alters.append(self.parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']+';')

            if diff['keyschain'][0::2][:2] == [u'TABLES', u'COLUMNS'] \
                    and diff['keyschain'][0::2][:3] != [u'TABLES', u'COLUMNS', u'COLUMN_KEY']:
                if diff['difftype']==u'added':
                    col_define = self.parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]
                    notnull = ''
                    if col_define[u'IS_NULLABLE']==u'NO':
                        notnull = 'NOT NULL'
                    default = ''
                    if col_define[u'COLUMN_DEFAULT'] is not None:
                        default = 'DEFAULT "{}"'.format(col_define[u'COLUMN_DEFAULT'])
                    if col_define[u'EXTRA']=='auto_increment':
                        default = u'AUTO_INCREMENT'


                    charset = ''
                    if col_define[u'CHARACTER_SET_NAME'] is not None:
                        charset = 'CHARACTER SET {}'.format(col_define[u'CHARACTER_SET_NAME'])
                    collate = ''
                    if col_define[u'COLLATION_NAME'] is not None:
                        collate = 'COLLATE {}'.format(col_define[u'COLLATION_NAME'])

                    #print col_define
                    a = 'ALTER TABLE {} ADD COLUMN {} {} {} {} {} {};'.format(diff['keyschain'][1], diff['keyschain'][3], col_define[u'COLUMN_TYPE'], charset, collate, notnull, default)
                    if a not in alters:
                        alters.append(a)

                if diff['difftype']==u'removed':
                    a = u'ALTER TABLE {} DROP COLUMN {};'.format(diff['keyschain'][1], diff['keyschain'][3])
                    if a not in alters:
                        alters.append(a)

                if diff['difftype']==u'differ':
                    col_define = self.parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]

                    after = ''
                    if diff['keyschain'][0::2][:3] == [u'TABLES', u'COLUMNS', u'PREVIOUS']:
                        if col_define['PREVIOUS'] is not None:
                            after = ' AFTER {0}'.format(col_define['PREVIOUS'])


                    notnull = u''
                    if col_define[u'IS_NULLABLE']==u'NO':
                        notnull = u'NOT NULL'
                    default = u''
                    if col_define[u'COLUMN_DEFAULT'] is not None:
                        default = u'DEFAULT "{}"'.format(col_define[u'COLUMN_DEFAULT'])
                    if col_define[u'EXTRA'] == 'auto_increment':
                        default = u'AUTO_INCREMENT'

                    charset = u''
                    if col_define[u'CHARACTER_SET_NAME'] is not None:
                        charset = u'CHARACTER SET {}'.format(col_define[u'CHARACTER_SET_NAME'])
                    collate = u''
                    if col_define[u'COLLATION_NAME'] is not None:
                        collate = u'COLLATE {}'.format(col_define[u'COLLATION_NAME'])
                    a = u'ALTER TABLE {} MODIFY COLUMN {} {} {} {} {} {} {};'.format(diff['keyschain'][1], diff['keyschain'][3], col_define[u'COLUMN_TYPE'], charset, collate, notnull, default, after)
                    if diff['keyschain'][1] not in modify_column_alters:
                        modify_column_alters[diff['keyschain'][1]] = []
                    alt = {'statement':a, 'column':diff['keyschain'][3], 'order': col_define[u'ORDINAL_POSITION']}
                    if alt not in modify_column_alters[diff['keyschain'][1]]:
                        modify_column_alters[diff['keyschain'][1]].append(alt)

            if diff['keyschain'][0::2][:2] == [u'TABLES', u'INDEXES']:
                if diff['difftype']==u'added' or diff['difftype']==u'differ':
                    idx_define = self.parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]
                    unique = ''
                    if idx_define['Non_unique']==0:
                         unique = 'UNIQUE'

                if diff['difftype']==u'added':
                    a = u'ALTER TABLE {} ADD {} KEY {} ({});'.format(diff['keyschain'][1], unique, diff['keyschain'][3], ', '.join(idx_define['COLUMNS']))
                    if a not in idx_alters:
                        idx_alters.append(a)

                if diff['difftype']==u'differ':
                    a = u'ALTER TABLE {} DROP KEY {};'.format(diff['keyschain'][1], diff['keyschain'][3])
                    if a not in alters:
                        idx_alters.append(a)

                    a = u'ALTER TABLE {} ADD {} KEY {} ({});'.format(diff['keyschain'][1], unique, diff['keyschain'][3], ', '.join(idx_define['COLUMNS']))
                    if a not in alters:
                        idx_alters.append(a)
                if diff['difftype']==u'removed':
                    a = u'ALTER TABLE {} DROP KEY {};'.format(diff['keyschain'][1], diff['keyschain'][3])
                    if a not in alters:
                        idx_alters.append(a)



            if diff['keyschain'][0::2][:2] == [u'TABLES', u'FOREIGN_KEYS']:
                if diff['difftype']==u'added':
                    const_define = self.parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]
                    a = u'ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({});'.format(diff['keyschain'][1], diff['keyschain'][3], const_define['column_name'], const_define['referenced_table_name'], const_define['referenced_column_name'])
                    if a not in alters:
                        idx_alters.append('SET foreign_key_checks = 0;')
                        idx_alters.append(a)
                        idx_alters.append('SET foreign_key_checks = 1;')

            if diff['keyschain'][0::2] == [u'ROUTINES'] and diff['difftype']==u'added':
                try:
                    #f2 = codecs.open('new/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                    #f2.write(unicode(diff['values'][1]))
                    proc_alters.append(unicode(self.parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']))
                    #f2.write(unicode(parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']))
                    #f2.close()
                except Exception as e:
                    print e


            if diff['keyschain'][0::2] == [u'ROUTINES', u'ROUTINE_DEFINITION'] or diff['keyschain'][0::2] == [u'ROUTINES', u'CREATE']:
                if diff['values'][0].replace('\x0D\x0A', '\x0A')!=diff['values'][1].replace('\x0D\x0A', '\x0A'):
                    try:
                        pass
                        #f1 = codecs.open('old/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                        #f1.write(unicode(diff['values'][0]))
                        #f1.write(unicode(loaded_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']))
                        #f1.close()
                    except Exception as e:
                        print e

                    try:
                        #f2 = codecs.open('new/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                        #f2.write(unicode(diff['values'][1]))
                        proc_alters.append(unicode(self.parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']))
                        #f2.write(unicode(parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']))
                        #f2.close()
                    except Exception as e:
                        print e

                else:
                    continue

            if diff['keyschain'][0::2] == [u'TABLES', u'TRIGGERS', u'ACTION_STATEMENT'] or diff['keyschain'][0::2] == [u'TABLES', u'TRIGGERS', u'CREATE']:
                if diff['values'][0].replace('\x0D\x0A', '\x0A')!=diff['values'][1].replace('\x0D\x0A', '\x0A'):
                    try:
                        pass
                        #f1 = codecs.open('old/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                        #f1.write(unicode(diff['values'][0]))
                        #f1.write(unicode(loaded_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]['CREATE']))
                        #f1.close()
                    except Exception as e:
                        print e

                    try:
                        #f2 = codecs.open('new/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                        #f2.write(unicode(diff['values'][1]))
                        proc_alters.append(unicode(self.parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]['CREATE']))
                        #f2.write(unicode(parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]['CREATE']))
                        #f2.close()
                    except Exception as e:
                        print e

                else:
                    continue

            if diff['keyschain'][0::2] == [u'TABLES', u'TRIGGERS']:
                if diff['difftype']==u'removed':
                    print diff['keyschain']
                    a = u'DROP TRIGGER IF EXISTS {};'.format(diff['keyschain'][3])
                    proc_alters.append(a)

            if diff['keyschain'][0::2] == [u'ROUTINES']:
                if diff['difftype']==u'removed':
                    routine_define = self.loaded_struct[diff['keyschain'][0]][diff['keyschain'][1]]
                    a = u'DROP {} IF EXISTS {};'.format(routine_define['ROUTINE_TYPE'],diff['keyschain'][1])
                    if a not in alters:
                        alters.append(a)
            self.formatted_diff.append(d.formatdiff(diff))

        #alters_f = codecs.open('./alters.sql','w', encoding='utf-8')

        for alter in alters:
            self.all_alters.append(alter)

        for alter in modify_column_alters:
            t_alters = modify_column_alters[alter]
            for a in sorted(t_alters, key=lambda x: x['order']):
                self.all_alters.append(a['statement'])

        for alter in idx_alters:
            self.all_alters.append(alter)
        for alter in proc_alters:
            self.all_alters.append(alter)

    def dump_alters(self, fd):
        if isinstance(fd, (str, unicode)):
            fd = codecs.open(fd, 'w', encoding='utf-8')
        try:
            for alter in self.all_alters:
                fd.write(alter+u'\n')
        finally:
            fd.close()

    def dump_formatted_diff(self, fd):
        if isinstance(fd, (str, unicode)):
            fd = codecs.open(fd, 'w', encoding='utf-8')
        try:
            for diff in self.formatted_diff:
                fd.write(diff+u'\n')
        finally:
            fd.close()
