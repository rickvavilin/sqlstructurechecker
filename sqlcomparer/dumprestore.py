import os
import csv
from MySQLdb import connect, OperationalError, Warning
from os import path
from warnings import filterwarnings

__author__ = 'Aleksandr Vavilin'


class MyDumpRestore(object):
    def __init__(self, host, username, password, workingdir, database, ignore=None, autocreate = True):
        self.host = host
        self.username = username
        self.password = password
        self.workingdir = workingdir
        self.database = database
        self.ignore = ignore or []
        self.errors = []
        self.db = None
        filterwarnings('ignore', category = Warning)

        try:
            self.connect_db()
        except OperationalError as e:
            self.create_database()
            self.connect_db()

    def create_database(self):
        db = connect(host=self.host, user=self.username, passwd=self.password, local_infile=1)
        cur = db.cursor()
        cur.execute('CREATE DATABASE {0} CHARACTER SET UTF8'.format(self.database))
        db.close()


    def connect_db(self):
        self.db = connect(host=self.host, user=self.username, passwd=self.password, db=self.database, local_infile=1)

    def tablelist(self):
        cur = self.db.cursor()
        cur.execute("SHOW FULL TABLES WHERE TABLE_TYPE LIKE 'BASE TABLE'")
        return [t[0] for t in cur.fetchall()]

    def filelist(self, file_dir):
        files = [f[0:-4] for f in os.listdir(path.join(self.workingdir, file_dir))]
        return [f for f in set(files)]

    def proclist(self):
        cur = self.db.cursor()
        cur.execute("SHOW PROCEDURE STATUS WHERE db='%s'" % self.database)
        return [t[1] for t in cur.fetchall()]

    def funlist(self):
        cur = self.db.cursor()
        cur.execute("SHOW FUNCTION STATUS WHERE db='%s'" % self.database)
        return [t[1] for t in cur.fetchall()]

    def viewlist(self):
        cur = self.db.cursor()
        cur.execute("SHOW FULL TABLES WHERE TABLE_TYPE LIKE 'VIEW'")
        return [t[0] for t in cur.fetchall()]

    def prepare_cursor(self):
        cur = self.db.cursor()
        cur.execute("SET NAMES 'utf8'")
        cur.execute("SET sql_mode = NO_AUTO_VALUE_ON_ZERO")
        return cur

    def apply_all(self, fun, objs):
        for t in objs:
            print t
            fun(t)

    def dump_table(self, table):
        cur = self.prepare_cursor()
        os.system("mysqldump --skip-comments -h%s -u%s -p%s -d -r%s/tables/%s.sql %s %s" % (
        self.host, self.username, self.password, self.workingdir, table, self.database, table))
        # TODO use subprocess to control exitcode
        if table not in self.ignore:
            cur.execute("""DESCRIBE %s""" % table)
            fields = [f[0] for f in cur.fetchall()]
            cur.execute("""SELECT * FROM %s""" % table)
            with open(path.join(self.workingdir, 'tables', '%s.csv' % table), "w") as f:
                writer = csv.writer(f, lineterminator='\n', quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(fields)
                writer.writerows(cur.fetchall())

    def dump_view(self, view):
        # TODO use subprocess to control exitcode
        os.system("mysqldump --skip-comments -h%s -u%s -p%s -d -r%s/views/%s.sql %s %s" % (
        self.host, self.username, self.password, self.workingdir, view, self.database, view))

    def dump_proc(self, proc):
        cur = self.prepare_cursor()
        cur.execute("""SHOW CREATE PROCEDURE `%s`""" % proc)
        pdef = "DROP PROCEDURE IF EXISTS `%s`;\nDELIMITER $$\n" % proc + cur.fetchall()[0][2] + "\n$$"
        with open(path.join(self.workingdir, "procs", "%s.sql" % proc), "w") as f:
            f.write(pdef)

    def dump_fun(self, fun):
        cur = self.prepare_cursor()
        cur.execute("""SHOW CREATE FUNCTION `%s`""" % fun)
        pdef = "DROP FUNCTION IF EXISTS `%s`;\nDELIMITER $$\n" % fun + cur.fetchall()[0][2] + "\n$$"
        with open(path.join(self.workingdir, "funs", "%s.sql" % fun), "w") as f:
            f.write(pdef)

    def restore_table(self, table):
        cur = self.prepare_cursor()
        try:
            os.system("mysql -h%s -u%s -p%s -D%s < %s/tables/%s.sql" % (self.host,
                                                                        self.username,
                                                                        self.password,
                                                                        self.database,
                                                                        self.workingdir,
                                                                        table))
            fname = path.join(self.workingdir, "tables", "%s.csv" % table)
            with open(fname, 'r') as f:
                fields = ",".join([c for c in f.readline()[1:-2].split('","')])
                cur.execute(
                    """LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\' IGNORE 1 LINES (%s)""" % (path.abspath(fname), table, fields))
        except Exception, e:
            self.errors.append("Error: {0} restoring {1}".format(e, table))
            print "Error: {0} restoring {1}".format(e, table)

    def restore_obj(self, filename):
        os.system("mysql -h%s -u%s -p%s -D%s < %s.sql" % (
        self.host, self.username, self.password, self.database, path.join(self.workingdir, filename)))

    def restore_tables(self):
        self.apply_all(self.restore_table, self.filelist('tables'))

    def restore_procs(self):
        self.apply_all(self.restore_obj, [path.join('procs', i) for i in self.filelist('procs')])

    def restore_funcs(self):
        self.apply_all(self.restore_obj, [path.join('funs', i) for i in self.filelist('funs')])

    def restore_views(self):
        self.apply_all(self.restore_obj, [path.join('views', i) for i in self.filelist('views')])

    def restore(self, object='all', name='*'):
        cur = self.prepare_cursor()
        cur.execute('DROP DATABASE IF EXISTS {0}'.format(self.database))
        self.db.close()
        self.create_database()
        self.connect_db()
        if object == 'all':
            self.restore_tables()
            self.restore_procs()
            self.restore_funcs()
            self.restore_views()
        elif object == 'table':
            if name == '*':
                self.restore_tables()
            else:
                self.restore_table(name)
        elif object == 'proc':
            if name == '*':
                self.restore_procs()
            else:
                self.restore_obj(path.join('procs', name))
        elif object == 'fun':
            if name == '*':
                self.restore_procs()
            else:
                self.restore_obj(path.join('funs', name))
        elif object == 'view':
            if name == '*':
                self.restore_procs()
            else:
                self.restore_obj(path.join('views', name))

    def prepare_dir(self, dir_name):
        try:
            os.mkdir(path.join(self.workingdir, dir_name))
        except OSError:
            pass
        for f in os.listdir(path.join(self.workingdir, dir_name)):
            try:
                os.unlink(path.join(self.workingdir, dir_name, f))
            except OSError as e:
                print e

    def prepare_dirs(self):
        try:
            os.mkdir(path.join(self.workingdir))
        except OSError:
            pass
        self.prepare_dir('tables')
        self.prepare_dir('procs')
        self.prepare_dir('funs')
        self.prepare_dir('views')

    def dump_tables(self):
        self.apply_all(self.dump_table, self.tablelist())

    def dump_procs(self):
        self.apply_all(self.dump_proc, self.proclist())

    def dump_funcs(self):
        self.apply_all(self.dump_fun, self.funlist())

    def dump_views(self):
        self.apply_all(self.dump_view, self.viewlist())

    def dump(self, object='all', name='*'):
        self.prepare_dirs()
        if object == 'all':
            self.dump_tables()
            self.dump_procs()
            self.dump_funcs()
            self.dump_views()
        elif object == 'structure':
            self.ignore = self.tablelist()
            self.dump_tables()
            self.dump_procs()
            self.dump_funcs()
            self.dump_views()
        elif object == 'table':
            if name == '*':
                self.dump_tables()
            else:
                self.dump_table(name)
        elif object == 'proc':
            if name == '*':
                self.dump_procs()
            else:
                self.dump_proc(name)
        elif object == 'fun':
            if name == '*':
                self.dump_funcs()
            else:
                self.dump_fun(name)
        elif object == 'view':
            if name == '*':
                self.dump_views()
            else:
                self.dump_view(name)
