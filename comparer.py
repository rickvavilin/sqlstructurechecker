__author__ = 'Aleksandr Vavilin'
import sqlcomparer
from sqlcomparer import dumprestore
import os
import json
import sys



if __name__=='__main__':
    workdir = './tmp'
    tmp_db_host = 'localhost'
    tmp_db_user = 'root'
    tmp_db_password = '2360087'
    try:
        os.mkdir(workdir)
    except OSError:
        pass
    config = json.load(open('./config.json', 'r'))
    reference_config = config['reference']
    instance_config = config['instance']
    if 'port' not in reference_config:
        reference_config['port'] = 3306
    if 'port' not in instance_config:
        instance_config['port'] = 3306
    tmp_reference_db = reference_config['database']+'_reference'
    tmp_instance_db = instance_config['database']+'_instance'
    reference_workingdir = os.path.join(workdir, 'reference')
    instance_workingdir = os.path.join(workdir, 'instance')
    reference_d = dumprestore.MyDumpRestore(reference_config['host'],
                                  reference_config['username'],
                                  reference_config['password'],
                                  reference_workingdir,
                                  reference_config['database'],
                                  port=reference_config['port'],
                                  ignore=config['ignore'])
    instance_d = dumprestore.MyDumpRestore(instance_config['host'],
                                  instance_config['username'],
                                  instance_config['password'],
                                  instance_workingdir,
                                  instance_config['database'],
                                  port=instance_config['port'],
                                  ignore=config['ignore'])
    reference_d.dump()
    instance_d.dump()

    reference_d = dumprestore.MyDumpRestore(tmp_db_host,
                                  tmp_db_user,
                                  tmp_db_password,
                                  reference_workingdir,
                                  tmp_reference_db,
                                  ignore=config['ignore'])
    instance_d = dumprestore.MyDumpRestore(tmp_db_host,
                                  tmp_db_user,
                                  tmp_db_password,
                                  instance_workingdir,
                                  tmp_instance_db,
                                  ignore=config['ignore'])
    reference_d.restore()
    instance_d.restore()
    reference_d.db.close()
    instance_d.db.close()

    sqlcomparer.savestructure(os.path.join(workdir, 'reference.json'), sqlcomparer.get_structure_from_database(tmp_db_host, tmp_db_user, tmp_db_password, database=tmp_reference_db))
    sqlcomparer.savestructure(os.path.join(workdir, 'instance.json'), sqlcomparer.get_structure_from_database(tmp_db_host, tmp_db_user, tmp_db_password, database=tmp_instance_db))
    c = sqlcomparer.Comparer(sqlcomparer.loadstructure(os.path.join(workdir, 'instance.json')),
                             sqlcomparer.loadstructure(os.path.join(workdir, 'reference.json')))
    c.compare()
    c.dump_alters('./alters.sql')
    c.dump_formatted_diff('./initial_diff.txt')
    os.system("mysql -h%s -u%s -p%s -D%s --force < ./alters.sql" % (tmp_db_host,
                                                            tmp_db_user,
                                                            tmp_db_password,
                                                            tmp_instance_db))
    sqlcomparer.savestructure(os.path.join(workdir, 'instance_after_alter.json'), sqlcomparer.get_structure_from_database(tmp_db_host, tmp_db_user, tmp_db_password, database=tmp_instance_db))
    c = sqlcomparer.Comparer(sqlcomparer.loadstructure(os.path.join(workdir, 'instance_after_alter.json')),
                             sqlcomparer.loadstructure(os.path.join(workdir, 'reference.json')))
    c.compare()
    c.dump_alters('./alters_last.sql')
    c.dump_formatted_diff('./final_diff.txt')


