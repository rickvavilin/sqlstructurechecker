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

    d = sqlcomparer.Differ()
    d.structdiff(loaded_struct, parsed_struct)

    filtered_diffs = d.filtereddiff(ignore)
    if len(filtered_diffs) == 0:
        print 'No differences found'

    for diff in filtered_diffs:
        if diff['keyschain'][0::2] == [u'TABLES']
        if diff['keyschain'][0::2] == [u'ROUTINES', u'ROUTINE_DEFINITION'] or diff['keyschain'][0::2] == [u'TABLES', u'TRIGGERS', u'ACTION_STATEMENT'] :
            if diff['values'][0].replace('\x0D\x0A', '\x0A')!=diff['values'][1].replace('\x0D\x0A', '\x0A'):
                f1 = codecs.open('old/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                f1.write(unicode(diff['values'][0]))
                f1.close()
                f2 = codecs.open('new/'+'_'.join(diff['keyschain'][1::2])+'.sql', 'wb', encoding='utf-8')
                f2.write(unicode(diff['values'][1]))
                f2.close()
            else:
                continue
        print d.formatdiff(diff)

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










