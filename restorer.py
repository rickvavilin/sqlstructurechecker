__author__ = 'rick'
from sqlcomparer import dumprestore

if __name__=='__main__':
    d = dumprestore.MyDumpRestore('127.0.0.1', 'root', '2360087', './160', 'odissey_new')
    d.restore('all')
    print d.errors
