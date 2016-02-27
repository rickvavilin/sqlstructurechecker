__author__ = 'rick'
from sqlcomparer import dumprestore

if __name__=='__main__':
    ignore = [
        'violations',
        'fragments',
        'not_imposed_stat',
        'log_user_actions',
        'proc_log',
        'post_batches',
        'operation_log',
        'operation_stages',
        'violations_stat',
        'stat_by_impose',
        'fssp_upload'
    ]
    d = dumprestore.MyDumpRestore('192.168.2.154', 'root', '2360087', './154', 'odissey', ignore=ignore)
    d.dump('all')

