# coding=utf-8
__author__ = 'Aleksandr Vavilin'
import sqlcomparer
import argparse
import codecs


def dump_from_db_to_file(args):
    sqlcomparer.savestructure(args.output, sqlcomparer.get_structure_from_database(**vars(args)))

def compare_files(args):
    compare(sqlcomparer.loadstructure(args.input), sqlcomparer.loadstructure(args.input2))

def compare_file_vs_database(args):
    compare(sqlcomparer.loadstructure(args.input), sqlcomparer.get_structure_from_database(**vars(args)))

def compare(loaded_struct, parsed_struct):
    ignore = sqlcomparer.default_ignore
    alters = []
    idx_alters = []
    proc_alters = []
    d = sqlcomparer.Differ()
    d.structdiff(loaded_struct, parsed_struct)

    filtered_diffs = d.filtereddiff(ignore)
    if len(filtered_diffs) == 0:
        print 'No differences found'

    for diff in filtered_diffs:
        if diff['keyschain'][0::2][:2] == [u'TABLES']:
            if diff['difftype']==u'removed':
                a = 'DROP TABLE {};'.format(diff['keyschain'][1])
                if a not in alters:
                    alters.append(a)
            if diff['difftype']==u'added':
                alters.append(parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']+';')

        if diff['keyschain'][0::2][:2] == [u'TABLES', u'COLUMNS'] \
                and diff['keyschain'][0::2][:3] != [u'TABLES', u'COLUMNS', u'COLUMN_KEY']:
            if diff['difftype']==u'added':
                col_define = parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]
                notnull = ''
                if col_define[u'IS_NULLABLE']==u'NO':
                    notnull = 'NOT NULL'
                default = ''
                if col_define[u'COLUMN_DEFAULT'] is not None:
                    default = 'DEFAULT "{}"'.format(col_define[u'COLUMN_DEFAULT'])

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
                '''
                if col_define[u'COLUMN_KEY']=='MUL':
                    a = 'ALTER TABLE {} ADD KEY {} ({});'.format(diff['keyschain'][1], diff['keyschain'][3], diff['keyschain'][3])
                    if a not in alters:
                        alters.append(a)
                if col_define[u'COLUMN_KEY']=='UNI':
                    a = 'ALTER TABLE {} ADD UNIQUE KEY {} ({});'.format(diff['keyschain'][1], diff['keyschain'][3], diff['keyschain'][3])
                    if a not in alters:
                        alters.append(a)
                '''
            if diff['difftype']==u'removed':
                a = u'ALTER TABLE {} DROP COLUMN {};'.format(diff['keyschain'][1], diff['keyschain'][3])
                if a not in alters:
                    alters.append(a)

            if diff['difftype']==u'differ':
                col_define = parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]

                after = ''
                if diff['keyschain'][0::2][:3] == [u'TABLES', u'COLUMNS', u'ORDINAL_POSITION']:
                    pass


                notnull = u''
                if col_define[u'IS_NULLABLE']==u'NO':
                    notnull = u'NOT NULL'
                default = u''
                if col_define[u'COLUMN_DEFAULT'] is not None:
                    default = u'DEFAULT "{}"'.format(col_define[u'COLUMN_DEFAULT'])
                if col_define[u'EXTRA']=='auto_increment':
                    default = u'AUTO_INCREMENT'

                charset = u''
                if col_define[u'CHARACTER_SET_NAME'] is not None:
                    charset = u'CHARACTER SET {}'.format(col_define[u'CHARACTER_SET_NAME'])
                collate = u''
                if col_define[u'COLLATION_NAME'] is not None:
                    collate = u'COLLATE {}'.format(col_define[u'COLLATION_NAME'])
                a = u'ALTER TABLE {} MODIFY COLUMN {} {} {} {} {} {} {};'.format(diff['keyschain'][1], diff['keyschain'][3], col_define[u'COLUMN_TYPE'], charset, collate, notnull, default, after)
                if a not in alters:
                    alters.append(a)

        if diff['keyschain'][0::2][:2] == [u'TABLES', u'INDEXES']:
            idx_define = parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]
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
                const_define = parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]
                a = u'ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({});'.format(diff['keyschain'][1], diff['keyschain'][3], const_define['column_name'], const_define['referenced_table_name'], const_define['referenced_column_name'])
                if a not in alters:
                    idx_alters.append('SET foreign_key_checks = 0;')
                    idx_alters.append(a)
                    idx_alters.append('SET foreign_key_checks = 1;')

        if diff['keyschain'][0::2] == [u'ROUTINES'] and diff['difftype']==u'added':
            try:
                f2 = codecs.open('new/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                #f2.write(unicode(diff['values'][1]))
                proc_alters.append(unicode(parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']))
                f2.write(unicode(parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']))
                f2.close()
            except Exception as e:
                print e


        if diff['keyschain'][0::2] == [u'ROUTINES', u'ROUTINE_DEFINITION'] or diff['keyschain'][0::2] == [u'ROUTINES', u'CREATE']:
            if diff['values'][0].replace('\x0D\x0A', '\x0A')!=diff['values'][1].replace('\x0D\x0A', '\x0A'):
                try:
                    f1 = codecs.open('old/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                    #f1.write(unicode(diff['values'][0]))
                    f1.write(unicode(loaded_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']))
                    f1.close()
                except Exception as e:
                    print e

                try:
                    f2 = codecs.open('new/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                    #f2.write(unicode(diff['values'][1]))
                    proc_alters.append(unicode(parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']))
                    f2.write(unicode(parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]]['CREATE']))
                    f2.close()
                except Exception as e:
                    print e

            else:
                continue

        if diff['keyschain'][0::2] == [u'TABLES', u'TRIGGERS', u'ACTION_STATEMENT'] or diff['keyschain'][0::2] == [u'TABLES', u'TRIGGERS', u'CREATE']:
            if diff['values'][0].replace('\x0D\x0A', '\x0A')!=diff['values'][1].replace('\x0D\x0A', '\x0A'):
                try:
                    f1 = codecs.open('old/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                    #f1.write(unicode(diff['values'][0]))
                    f1.write(unicode(loaded_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]['CREATE']))
                    f1.close()
                except Exception as e:
                    print e

                try:
                    f2 = codecs.open('new/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                    #f2.write(unicode(diff['values'][1]))
                    proc_alters.append(unicode(parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]['CREATE']))
                    f2.write(unicode(parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]['CREATE']))
                    f2.close()
                except Exception as e:
                    print e

            else:
                continue

        if diff['keyschain'][0::2] == [u'ROUTINES']:
            if diff['difftype']==u'removed':
                routine_define = loaded_struct[diff['keyschain'][0]][diff['keyschain'][1]]
                a = u'DROP {} IF EXISTS {};'.format(routine_define['ROUTINE_TYPE'],diff['keyschain'][1])
                if a not in alters:
                    alters.append(a)
        #if diff['keyschain'][0::2][:2] == [u'TABLES', u'CONSTRAINTS']:
        #    if diff['difftype']==u'added':
        #        const_define = parsed_struct[diff['keyschain'][0]][diff['keyschain'][1]][diff['keyschain'][2]][diff['keyschain'][3]]
        #        if const_define[u'CONSTRAINT_TYPE']==u'UNIQUE':
        #            a = 'ALTER TABLE {} ADD UNIQUE KEY {} ({});'.format(diff['keyschain'][1],
        print d.formatdiff(diff)

    alters_f = codecs.open('./alters.sql','w', encoding='utf-8')
    for alter in alters:
        alters_f.write(alter+u'\n')
    for alter in idx_alters:
        alters_f.write(alter+u'\n')
    for alter in proc_alters:
        alters_f.write(alter+u'\n')


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










