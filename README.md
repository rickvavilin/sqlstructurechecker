# README #


### SQL compare tool ###

* Scripts for SQL schemas compare and generate alters

### Как это работает ###
## Автоматическая генерация SQL скрипта миграции##
1. Структура БД-источника и БД-цели выгружаются в структуру директорий вида

        -source
            -tables
            -procs
            -funs
            -views
            
где содержатся sql скрипты для создания соответствующих объектов БД. Триггеры и индексы создаются одновременно с таблицами.

2. На основе выгруженных структур создаются временные БД (по умолчанию mysql должен быть запущен на localhost)

3. Производится анализ структуры временных БД, создается скрипт миграции alters.sql, и файл initial_diff.txt

4. Скрипт alters.sql применяется к временной БД, по структуре соответствующей БД source

5. Производится анализ структуры временных БД, создается скрипт миграции alters_last.sql, и файл final_diff.txt

6. Если final_diff.txt не пуст, то необходимо вручную внести изменения в скрипт миграции

 

            

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
            }
            tmp_db_password:"" //пароль для временно БД, где будет происходить сравнение схем
        }
1. Запустить python comparer.py

В текущей директории будут созданы файлы:

* initial_diff.txt - список несоответствий в изначальных схемах БД 

* alters.sql - скрипт преобразующий схему source в target

* final_diff.txt - список несоответствий после наложения alters.sql. При полностью корректном исполнении скрипта формируется пустой файл

* alters_last.sql - скрипт преобразующий схему source в target, на основе final_diff.txt. При полностью корректном исполнении скрипта формируется пустой файл
