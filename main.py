__author__ = 'Aleksandr Vavilin'
from MySQLdb import connect, cursors
import tabulate

db = connect(host='localhost', user='root', passwd='2360087', db='information_schema')
cur = db.cursor(cursors.DictCursor)

cur.execute("SET CHARSET 'utf8'")

cur.execute('select * from tables where table_schema=%s;',['structure_test'])

tables = []

print tabulate.tabulate(cur.fetchall(), headers=[d[0] for d in cur.description])

