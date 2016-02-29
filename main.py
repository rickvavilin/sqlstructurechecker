# coding=utf-8
__author__ = 'Aleksandr Vavilin'
import sqlcomparer
import argparse
import codecs


def dump_from_db_to_file(args):
    sqlcomparer.savestructure(args.output, sqlcomparer.get_structure_from_database(**vars(args)))



def compare_files(args):
    c = sqlcomparer.Comparer(sqlcomparer.loadstructure(args.input), sqlcomparer.loadstructure(args.input2))
    c.compare()
    c.dump_alters('./alters.sql')


def compare_file_vs_database(args):
    c = sqlcomparer.Comparer(sqlcomparer.loadstructure(args.input), sqlcomparer.get_structure_from_database(**vars(args)))
    c.compare()
    c.dump_alters('./alters.sql')





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










