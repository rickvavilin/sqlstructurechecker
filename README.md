# README #


### SQL compare tool ###

* Scripts for SQL schemas compare and generate alters

### Использование ###
1. Создать файл config.json

        {
            "target":{ //target - БД-образец, alter-операторы будут сгенерированы так, чтоб преобразовать source в target
                "type": "db", //[dir, db]
                "host": "", //IP адрес или имя хоста для подключения
                "username": "", //имя пользователя
                "port": 3316, // порт ( не обязательный)
                "password": "", // пароль для подключения
                "database": "" // имя БД
            },
            "source":{
                "type": "dir",//[dir, db]
                "dir":"/foo/bar", //путь к директории со структурой БД
                "database": ""// имя БД
            },
            "ignore":[ //список таблиц, из которых не будут выгружаться данные
                "violations",
                "fragments",
                "not_imposed_stat",
                "log_user_actions",
                "proc_log",
                "post_batches",
                "operation_log",
                "operation_stages",
                "violations_stat",
                "stat_by_impose",
                "fssp_upload"
            ]
        }
1. Запустить python comparer.py

В текущей директории будут созданы файлы:

* initial_diff.txt - список несоответствий в изначальных схемах БД 

* alters.sql - скрипт преобразующий схему source в target

* final_diff.txt - список несоответствий после наложения alters.sql. При полностью корректном исполнении скрипта формируется пустой файл

* alters_last.sql - скрипт преобразующий схему source в target, на основе final_diff.txt. При полностью корректном исполнении скрипта формируется пустой файл
