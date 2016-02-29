__author__ = 'Aleksandr Vavilin'
import sqlcomparer
from sqlcomparer import dumprestore
import os
import json
import shutil
import sys
import paramiko

if __name__=='__main__':
    workdir = './tmp'
    tmp_db_host = 'localhost'
    tmp_db_user = 'root'
    try:
        os.mkdir(workdir)
    except OSError:
        pass
    config_name = './config-dir.json'
    if len(sys.argv)>1:
        config_name = sys.argv[1]
    config = json.load(open(config_name, 'r'))
    tmp_db_password = config['tmp_db_password']

    target_config = config['target']
    source_config = config['source']
    if 'port' not in target_config:
        target_config['port'] = 3306
    if 'port' not in source_config:
        source_config['port'] = 3306
    tmp_target_db = target_config['database'] + '_target'
    tmp_source_db = source_config['database'] + '_source'
    target_workingdir = os.path.join(workdir, 'target')
    source_workingdir = os.path.join(workdir, 'source')
    shutil.rmtree(target_workingdir, ignore_errors=True)
    shutil.rmtree(source_workingdir, ignore_errors=True)
    if 'type' not in target_config or target_config['type'] == 'db':
        target_d = dumprestore.MyDumpRestore(target_config['host'],
                                             target_config['username'],
                                             target_config['password'],
                                             target_workingdir,
                                             target_config['database'],
                                             port=target_config['port'])
        target_d.dump('structure')
    elif target_config['type'] == 'dir':
        shutil.copytree(target_config['dir'], target_workingdir)
    else:
        raise Exception("target dbtype {} incorrect".format(target_config['type']))

    if 'type' not in source_config or source_config['type'] == 'db':
        source_d = dumprestore.MyDumpRestore(source_config['host'],
                                             source_config['username'],
                                             source_config['password'],
                                             source_workingdir,
                                             source_config['database'],
                                             port=source_config['port'])
        source_d.dump('structure')
    elif source_config['type'] == 'dir':
        shutil.copytree(source_config['dir'], source_workingdir)
    else:
        raise Exception("instance dbtype {} incorrect".format(source_config['type']))



    target_d = dumprestore.MyDumpRestore(tmp_db_host,
                                         tmp_db_user,
                                         tmp_db_password,
                                         target_workingdir,
                                         tmp_target_db)
    source_d = dumprestore.MyDumpRestore(tmp_db_host,
                                         tmp_db_user,
                                         tmp_db_password,
                                         source_workingdir,
                                         tmp_source_db)
    target_d.restore()
    source_d.restore()
    target_d.db.close()
    source_d.db.close()

    sqlcomparer.savestructure(os.path.join(workdir, 'target.json'), sqlcomparer.get_structure_from_database(tmp_db_host, tmp_db_user, tmp_db_password, database=tmp_target_db))
    sqlcomparer.savestructure(os.path.join(workdir, 'source.json'), sqlcomparer.get_structure_from_database(tmp_db_host, tmp_db_user, tmp_db_password, database=tmp_source_db))
    c = sqlcomparer.Comparer(sqlcomparer.loadstructure(os.path.join(workdir, 'source.json')),
                             sqlcomparer.loadstructure(os.path.join(workdir, 'target.json')))
    c.compare()
    c.dump_alters('./alters.sql')
    c.dump_formatted_diff('./initial_diff.txt')
    os.system("mysql -h%s -u%s -p%s -D%s --force < ./alters.sql" % (tmp_db_host,
                                                                    tmp_db_user,
                                                                    tmp_db_password,
                                                                    tmp_source_db))
    sqlcomparer.savestructure(os.path.join(workdir, 'source_after_alter.json'), sqlcomparer.get_structure_from_database(tmp_db_host, tmp_db_user, tmp_db_password, database=tmp_source_db))
    c = sqlcomparer.Comparer(sqlcomparer.loadstructure(os.path.join(workdir, 'source_after_alter.json')),
                             sqlcomparer.loadstructure(os.path.join(workdir, 'target.json')))
    c.compare()
    c.dump_alters('./alters_last.sql')
    c.dump_formatted_diff('./final_diff.txt')


