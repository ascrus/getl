# Содержание
* [Кратко про Getl](#about)
  * [Исходники и документация](#source)
  * [Примеры работы](#demo)
  * [Решаемые задачи](#works)
* [Начало работы с Getl](#start_getl)
  * [Подключение Getl к проекту с помощью Gradle](#gradle)
  * [Использование в классах и скриптах Groovy](#groovy)
* [Работа с источниками данных](#data_sources)
  * [Работа с соединениями](#connections)    
  * [Работа с наборами данных](#datasets)
* [Etl операторы работы с источниками данных](#etl_operation)
  * [Копирование записей из источника в приёмник с помощью оператора copyRows](#etl_copy)
  * [Операторы записи в наборы данных rowsTo и rowsToMany](#etl_write)
  * [Оператор чтения из наборов данных rowsProcess](#etl_read)
* [Выполнение пакетных сценариев SQL](#sql)
* [Расширения в Getl под Micro Focus Vertica](#vertica)
* [Работа с файловыми системами](#fileman)
  * [Копирование файлов](#fileman_copy)
  * [Очистка файлов](#fileman_clean)
  * [Парсинг файлов](#fileman_processing)
* [Работа со стендами](#devops)
* [Разработка unit тестов](#unit_tests)
* [Модели Getl](#models)
  * [Модель эталонных файлов referensFiles](#model_ref_files)
  * [Модель эталонных данных Vertica referenceVerticaTables](#model_ref_ver_tables)
  * [Модель набора таблиц setOfTables](#model_set_tables)
  * [Модель маппинга между двумя источниками mapTables](#model_map_tables)

# <a name="about"></a>Кратко про Getl
Groovy ETL (Getl) - open source проект на Groovy, разрабатываемый с 2012 года для автоматизации загрузки и обработки
данных из разных источников.

Getl является заменой классических ETL для случаев, когда в проекте требуется быстрая разработка процессов загрузки данных
из разнообразных файловых источников со сложными форматами и таблиц РСУБД. В Getl не нужно разрабатывать для каждой таблицы
или формата файла свой процесс. Простота и функционал ETL языка позволяют создавать шаблоны логики работы с данными
в соответствии с программной архитектурой проекта, а затем вызывать их для заданных источников. Getl является
интеллектуальным ETL инструментом и самостоятельно создаст правильный маппинг между копируемыми данными, приведет к нужным
типам и выполнит задачу, скомпилировав ее на лету в байт код для выполнения на Java.

Дополнительно, для Micro Focus Vertica в Getl содержится расширенная функциональность, которая позволяет реализовывать
сложные решения для захвата, доставки и расчета данных для этого хранилища данных.

Getl является базовой платформой ETL инструментов компании EasyData. Он позволяет быстро и качественно решать задачи любой сложности
по захвату, доставке и обработке данных. Продукт постоянно развивается под новые задачи и проекты и является частью ПО
управления жизненным циклом хранилищ данных Micro Focus Vertica, разрабатываемым в EasyData.

## <a name="source"></a>Исходники и документация
* Исходный код проекта располагается в GitHub проекте [getl](https://github.com/ascrus/getl).
* На базе Getl разработана библиотека шаблонов для облегчения разработки задач под Micro Focus Vertica, исходные коды
  которой выложены на GitHub в проекте [getl.vertica](https://github.com/ascrus/getl.vertica).

## <a name="demo"></a>Примеры работы
* Примеры работы с классами Getl и Dsl языком можно найти в
  [unit-тестах](https://github.com/ascrus/getl/tree/master/src/test/groovy/getl/lang) Getl.
* Примеры работы с Getl на базе H2 Database можно посмотреть на GitHub проекте
  [Getl examples](https://github.com/ascrus/getl-examples).
* В качестве примеров разработки шаблонов можно использовать исходные коды библиотеки шаблонов
  [getl.vertica](https://github.com/ascrus/getl.vertica).

## <a name="works"></a>Решаемые задачи
Getl полезен, если требуется:
* Копировать массивы данных между разными JDBC совместимыми базами данных и файловыми источниками;
* Обеспечить инкрементальный захват данных и загрузку изменений в хранилища данных;
* Копировать, обрабатывать или удалять по заданным условиям файлы на файловых источниках;
* Запускать пилотные проекты для хранилищ данных, с быстрым развертыванием таблиц в БД и их наполнением данных с источников;
* Организовывать запуск и отладку ETL процессов между средами разработки, тестирования и эксплуатации;
* Для облегчения тестирования создать модели эталонных данных и файлов и автоматизировать инициализацию данными для
  запускаемых задач из unit-тестов;
* Для контроля состояния таблиц в хранилище данных описать правила времени из заполнения и автоматизировать процесс
  мониторинга данных в таблицах;
* Разрабатывать для автоматизации процессов собственные шаблоны для захвата, загрузки и обработки данных под
  используемую в проекте бизнес логику согласно выбранной архитектуре хранилища данных.

# <a name="start_getl">Начало работы с Getl
Для того, чтобы начать писать на Getl процессы ETL, нужно на базовом уровне изучить два продукта:
1. Groovy, краткое руководство на русском можно прочитать на "Хабрахабр": [статья Groovy за 15 минут](http://habrahabr.ru/post/122127/).
1. Любой IDE, который поддерживает Java и Groovy и обеспечивает комфортную разработку и отладку кода, например
   [JetBrains IntelliJ IDEA](https://www.jetbrains.com/ru-ru/idea/documentation/) или
   [Eclipse](https://www.ibm.com/developerworks/ru/library/os-eclipse-platform/).

## <a name="gradle">Подключение Getl к проекту с помощью Gradle
Для подключения к проекту на "_gradle_" достаточно прописать в файл "_build.gradle_" проекта ссылку на Getl (сверяйте
номер актуальной версии в Maven):
```groovy
dependencies {
  compile(group: 'net.sourceforge.getl', name: 'getl', version:'4.7.*')
}
```
* Getl выкладывается в Maven Central, поэтому указывать репозиторий не нужно.
* Getl уже содержит ссылки на все зависимые библиотеки, поэтому в проект будут подтянуты Groovy и остальные библиотеки,
  требуемые для его работы.

Для работы с JDBC драйверами РСУБД в проекте рекомендуется сделать директорию "_jdbc_" и скопировать туда
их jar файлы. Тогда в gradle можно указать их подключение для запуска и отладки проекта в IDE:
```groovy
dependencies {
  compile(group: 'net.sourceforge.getl', name: 'getl', version:'4.7.*')
  compile fileTree(dir: 'jdbc')
}
```
P.S. Если проекту будет нужно одновременно использовать разные версии JDBC драйвера, в Getl возможно при описании соединения к РСУБД указать в свойстве "_driverPath_"
путь к конкретному jar файлу, который требуется использовать для этого соединения.

## <a name="groovy">Использование в классах и скриптах Groovy
Для использования Getl в классах Groovy достаточно использовать статический метод Dsl класса Getl. Внутри кода Dsl доступны все функции Getl и можно разрабатывать любые сценарии:
```groovy
package demo

import getl.lang.Getl

class GroovyApp {
  static void main(def args) {
    helloWorld()
  }

  void helloWorld() {
    Getl.Dsl {
      logInfo 'Привет мир!'
    }
  }
}
```
Вызов программы из командной строки будет такой же, как для любого Java приложения. Для этого нужно скомпилировать проект в jar файл с
помощью Gradle и вызывать с указанием запускаемого класса:
```shell script
java -cp myproj.jar demo.GroovyApp
```
P.S. В class path (-cp) также требуется добавить пути к jar файлам Getl и используемых JDBC драйверов.

Классы удобно использовать там, где требуются возможности ООП (наследование, ОРМ, библиотеки статических функций и т. д.). Для ETL/ELT процессов это не требуется и усложняет разработку.
Помимо классов, Groovy поддерживает разработку кода в виде сценариев (скриптов):
```groovy
// Файл GetlDemoScript.groovy
package demo

import groovy.transform.BaseScript
import getl.lang.Getl
import groovy.transform.Field

@BaseScript Getl main

@Field String name = 'приятель'

logInfo "Привет $name!"
``` 
1. Аннотация "_BaseScript_" указывает Groovy, что скрипт будет выполняться на Dsl языке Getl. Groovy автоматически при компиляции создаст Java класс на этот сценарий,
   который наследуется от Getl и реализует всю логику скрипта в методе "_run_" этого класса.
1. Переменная "_main_" при работе скрипта будет содержать ссылку на экземпляр объекта класса сценария. В случаях, когда локальные методы или переменные будут перекрывать именованием
   методы и переменные Getl, переменная _main_ позволит все равно получить к ним доступ.
1. Переменная "_name_" с дескриптором "_@Field_" является параметром, который можно задать при вызове скрипта из командной строки или из другого места кода.

Сценарий можно вызвать даже без компиляции с помощью Groovy, если он установлен в ОС. Для этого нужно скопировать по пути инсталляции Groovy в директорию "_libs_" все jar файлы,
нужные для работы Getl, включая JDBC драйвера. Рекомендуется вызывать скрипт из главной директории проекта, если в проекте используются пакеты:
```shell script
groovy demo/GetlDemoScript.groovy vars.name=друг
```

Для разработки сложных проектов рекомендуется использовать IDE с поддержкой Java и Groovy. В таком случае становится доступна полноценная разработка, отладка и сборка проекта в jar файл.
Getl поддерживает хранение конфигурационных файлов и SQL скриптов в ресурсных файлах проекта, это облегчает сборку и сопровождение проектов. Для запуска сценариев из jar файла в классе
Getl реализован метод "_main_" для запуска приложений, которому можно в параметре "_runclass_" передать полное имя скрипта Groovy из проекта для запуска:
```shell script
java -cp myproj.jar getl.lang.Getl runclass=demo.GetlDemoScript vars.name=друг
``` 
P.S. В class path (-cp) также требуется добавить пути к jar файлам Getl и используемых JDBC драйверов.

# <a name="data_sources"></a>Работа с источниками данных
Для работы с источниками данных в Getl поддерживаются следующие виды объектов:
* соединения (connection): содержат url или путь и служат для подключения к источнику данных 
  (например h2Connection, csvConnection, oracleConnection и т.д.);
* наборы данных (dataset): содержат описание хранения наборов данных в источниках, имена таблиц или файлов, список полей 
  (например h2Table, csv, oracleTable и т.д.);
* счетчики (sequence): содержат указатель на счетчик РСУБД, генерирующий уникальные числовые значения для идентификаторов;
* инкрементальные точки (historyPoints): содержат указатель на таблицу РСУБД, в которой сохраняется история инкрементального 
  захвата данных из источников. 

## <a name="connections"></a>Работа с соединениями
Соединения бывают трех видов:
* JDBC соединения к РСУБД: db2Connection, firebirdConnection, h2Connection, hiveConnection, impalaConnection, mssqlConnection, 
  mysqlConnection, netezzaConnection, netsuiteConnection, oracleConnection, postgresqlConnection, verticaConnection;
* Файловые форматы: csvConnection, excelConnection, jsonConnection, xmlConnection, yamlConnection;
* Облачные системы: salesforceConnection, kafkaConnection.

Над соединениями можно производить следующие операции:
* Хранить список логинов в _storedLogins_ и переключаться между ними во время работы с помощью метода _useLogin_;
* Устанавливать и разрывать соединение с помощью методов _connect_ и _disconnect_ или установки свойства _connected_;
* Управлять транзакциями JDBC соединений с помощью методов _startTran_, _commitTran_, _rollbackTran_ и _transaction_;
* Выполнять параметризированные SQL операторы для JDBC соединений с помощью команды _executeCommand_;
* Получать список таблиц и представлений из JDBC источников с помощью метода _retriveDatasets_.

## <a name="datasets"></a>Работа с наборами данных
Наборы данных бывают трех видов:
* Таблицы, запросы и представления JDBC соединений: query, view, db2Table, firebirdTable, h2Table, hiveTable, impalaTable, 
  mssqlTable, mysqlTable, netezzaTable, netsuiteTable, oracleTable, postgresqlTable, verticaTable;
* Файловые структуры: csv, excel, json, xml, yaml;
* Облачные системы: salesforce, salesforceQuery, kafka.

В Getl поддерживается чтение и запись данных в наборы данных, но для разных источников есть ограничения:
* Не поддерживается вставка записей в Hive;
* Не поддерживается изменение и удаление записей в Hive, Impala и Kafka;
* Пакетная загрузка файлов поддерживается только для РСУБД H2, Hive, Impala и Vertica;
* Не поддерживается запись данных в файлы формата XML, YAML и Excel;
* Не поддерживается вставка, изменение и удаление записей в NetSuite и SalesForce.

Над наборами данных можно производить следующие операции:
* Создавать новые таблицы в JDBC источниках с помощью метода _create_;
* Удалять таблицы в JDBC источниках и файловые наборы данных с помощью метода _drop_;
* Получать описание полей таблиц JDBC источников с помощью метода _retriveFields_
* Обрабатывать записи с помощью метода _eachRow_ и получать список записей в память функцией _rows_;
* Записывать записи с помощью методов _openWrite_, _write_, _doneWrite_ и _closeWrite_;
* Очищать таблицы JDBC источников с помощью метода _truncate_.

# <a name="etl_operation"></a>Etl операторы работы с источниками данных
Для копирования и трансформации данных между источниками используются операторы из секции etl:
* etl.copyRows: копирование записей из одного набора данных в другой с возможностью маппинга и трансформации данных 
  (например выгрузка из таблицы СУБД всех записей в файл CSV для его дальнейшей пакетной загрузки в другой источник);
* etl.rowsTo: запись информации в набор данных (например генерация в таблицу случайных данных для тестирования);
* etl.rowsToMany: запись информации в несколько наборов данных (например парсинг json файла и запись в таблицы master-detail);
* etl.processRows: чтение записей из набора данных (например проверка поступившего xml файла и формирование файла ошибочных записей).

## <a name="etl_copy"></a>Копирование записей из источника в приёмник с помощью оператора copyRows
В Getl реализован интеллектуальный менеджер копирования данных между источниками, в котором поддерживается:
* Автоматическая связь полей источника и приёмника по их именам с возможностью отключения функции;
* Автоматическое приведение типов значений копируемых полей записей из источника в приёмник с возможностью отключения функции;
* Указание связи копируемых полей вручную;
* Проверка, преобразование и обработка копируемых записей в собственном коде;
* Поддержка отложенного копирования во временные CSV файлы с дальнейшей пакетной загрузкой полученных файлов в БД приёмника;
* Оптимизация скорости копирования наборов данных с помощью компиляции кода под конкретные структуры наборов данных в байт код для его выполнения на JVM.

Пример копирования всех записей между наборами данных с одинаковыми полями:
```groovy
// Объявление таблицы Oracle
def oratab = oracleTable {
  useConnection oracleConnection { // Объявление соединения Oracle для таблицы
    connectHost = 'oracle-host'
    connectDatabase = 'oradb'
    login = 'user'
    password = 'password'
  }
  schemaName = 'user'
  tableName = 'table1'
}

// Объявление таблицы Vertica
def vertab = verticaTable {
  useConnection verticaConnection { // Объявление соединения Vertica для таблицы
    connectHost = 'vertica-host1'
    connectDatabase = 'verdb'
    extended.backupservernode = 'vertica-host2,vertica-host3'
    login = 'user'
    password = 'password'
  }
  schemaName = 'stage'
  tableName = 'table1'
}

// Копирование всех записей из Oracle в Vertica
etl.copyRows(oratab, vertab)
```

Пример копирования всех таблиц из указанной схемы Oracle в таблицы с аналогичной структурой для схемы Vertica в многопоточном режиме. Перед копированием создаются таблицы Vertica,
копирование производится с помощью пакетной загрузки в таблицы Vertica:
```groovy
verticaConnection('ver', true) {
  connectHost = 'vertica-host1'
  connectDatabase = 'verdb'
  login = 'user'
  password = 'password'
  schemaName = 'stage'
}

oracleConnection('ora', true) {
  connectHost = 'oracle-host'
  connectDatabase = 'oradb'
  login = 'user'
  password = 'password'

  // Получение списка таблиц из схемы "schema" и добавление их в репозиторий в группу "ora.schema"
  addTablesToRepository retrieveDatasets(schemaName: 'schema', 'ora.schema')

  // Многопоточный режим работы
  thread {
    // Использовать для обработки потоков имена таблиц Oracle из репозитория
    useList listJdbcTables('ora.schema:*')
    // Выполнять обработку списка таблиц в четыре потока
    countProc = 4
    // Остановить все потоки, если один из них получил ошибку во время выполнения
    abortOnError = true

    // Код обработки имени таблицы из списка в потоке
    run { tableName ->
      // Обрабатываемая таблица Oracle
      def oratab = oracleTable(tableName)

      // Описание таблицы Vertica
      def vertab = verticaTable {
        // Использовать соединение "ver"
        useConnection verticaConnection('ver')
        tableName = oratab.tableName
        // Адаптировать поля Oracle к типам Vertica
        setOracleFields oratab
        // Создать таблицу
        create()
      }
      // Копировать записи из Oracle в Vertica 
      etl.copyRows(oratab, vertab) {
        // Включить копирование через промежуточные CSV файлы и пакетную загрузку в приёмник
        bulkLoad = true
        // Выгружать из исходной таблицы в сжатые по GZ алгоритму CSV файлы
        bulkAsGZIP = true
      }
    }
  }
}
```

## <a name="etl_write">Операторы записи в наборы данных rowsTo и rowsToMany
Для записи в источники данных используются операторы "_etl.rowsTo_" и "_etl.rowsToMany_". Первый оператор позволяет записывать в указанную таблицу, второй оператор позволяет записывать
во множество таблиц сразу.

Пример записи данных в таблицу Vertica на базе случайно генерируемых данных:
```groovy
// Соединение к Vertica
def vercon = verticaConnetion {
  connectHost = 'vertica-host1'; connectDatabase = 'verdb'
  login = 'user'; password = 'password'
  schemaName = 'stage'

  // Выполнить запрос создания счетчика в БД
  executeCommand 'CREATE SEQUENCE IF NOT EXISTS stage.s_sales INCREMENT BY 1000;'
}

// Описание счетчика
def seq = sequence {
  // Установить соединение
  useConnection vercon
  // Полное имя в БД
  name = 'stage.s_sales'
  // Кэшировать получение значений с шагом 1000 чисел
  cache = 1000
}

// Таблица Vertica
verticaTable {
  // Установить соединение
  useVerticaConnection vercon

  // Имя в БД (по умолчанию для Vertica будет использоваться схема данных "public")
  tableName = 'random_data'
  // Поля таблицы
  field('id') { type = integerFieldType; isKey = true }
  field('name') { length = 50; isNull = false }
  field('dt') { type = datetimeType; isNull = false }
  field('value') { type = numericFieldType; length = 12; precision = 2 }
  // Создать таблицу
  if (!exists) create()

  // Запись данных в текущую таблицу
  etl.rowsTo {
    // Назначить количество записей в пакете для вставки
    destParams.batchSize = 1000
    writeRow { add -> // Код записи данных в дескриптор "add"
      (1..10000).each {
        def row = [:]
        row.id = seq.nextval()
        row.name = GenerationUtils.GenerateString(50)
        row.dt = GenerationUtils.GenerateDateTime()
        row.value = GenerationUtils.GenerateNumeric(12, 2)

        add row
      }
    }
  }
}
```

## <a name="etl_read">Оператор чтения из наборов данных rowsProcess
Для чтения и обработки записей наборов данных используется оператор _rowsProcess_. В отличие от eachRow наборов данных
этот оператор позволяет обрабатывать ситуации, когда полученные и источника записи содержат ошибки и собирать их 
в специальный набор данных, который можно использовать для анализа или хранения ошибочных записей.

# <a name="sql">Выполнение пакетных сценариев SQL
Для построения консолидированного слоя и разработки аналитических витрин Getl предлагает собственный язык хранимых процедур (ХП), реализуемый в операторе "_sql_".
В данном операторе поддерживается:
* Поочередное выполнение SQL операторов, разделенных точкой с запятой;
* Работа с переменными внутри скрипта;
* Установка значений переменных перед выполнением скрипта и получение их значений после выполнения в коде;
* Получение в переменную количества обработанных записей после выполнения DML оператора SQL;
* Команда _SET_ для получения значений в переменные из полей записи;
* Команда _IF_ для выполнения блока команд по условию;
* Команда _FOR_ для выполнения блока команд в цикле;
* Команда _BEGIN BLOCK_ для выполнения блока команд в оригинальном виде без обработки языком ХП;
* Команда _ERROR_ для аварийного завершения работы SQL скрипта с указанной ошибкой;
* Команда _EXIT_ для выхода из скрипта с указанным кодом;
* Команда _ECHO_ для вывода текста в консоль лога.

Для выполнения ХП скриптов используется метод "_exec_" в операторе "_sql_":
```groovy
// Соединение к Vertica
def vercon = verticaConnetion {
  connectHost = 'vertica-host1'
  connectDatabase = 'verdb'
  login = 'user'
  password = 'password'
}
// Выполнить SQL скрипт
sql {
  // Использовать указанное соединение Vertica
  useConnection vercon

  // Установить значение переменным для скрипта
  vars.param1 = 1
  vars.param2 = 'ok'
  vars.param3 = new Date()

  // Выполнить скрипт из текста
  exec '''
/*:count_row*/ 
UPDATE public.table1 
SET updatetime = '{param3}'::timestamp 
WHERE id = {param1} AND status = '{param2}'
    '''
  // Проверить полученное значение переменной из скрипта
  assert vars.count_row == 1, 'Не удалось обновить запись!'
}
```
При выполнении ХП Getl вместо {переменных} подставляет их значения. При использовании текстовых переменных внутри скриптов следует их
обрамлять в одинарные кавычки. Для переменных с типом дата-время Getl подставляет текстовое значение в формате "yyyy-MM-dd HH:mm:ss".
Если перед DML оператором поставить "_/*:переменная*/_", то количество обработанных записей будет занесено в эту переменную и она будет доступна
в vars.

Скрипты можно выносить в отдельные файлы, в том числе ресурсные и вызывать их с помощью метода "_runFile_" в
операторе "_sql_".

Пример файла "/sql/script.sql", который располагается в ресурсах проекта:
```genericsql
-- Вывести сообщение на консоль
ECHO Расчет региона {regionid} ...

-- Получение периода расчета в переменные по именам полей из SELECT
SET SELECT Min(hour) as hour_start, Max(hour) as hour_finish, Count(*) AS count_rows
    FROM public.table1
    WHERE regionid = {regionid} AND status = 'incomplete';

-- Проверить, что нет записей для обработки или данные старше заданной даты
IF ({count_rows} = 0 OR '{hour_finish}'::timestamp < '2020-01-01'::timestamp);
    ECHO 'Записей для расчета не найдено!'
    // Прекращение работы SQL скрипта
    EXIT;
END IF;

ECHO Выявлено {count_rows} записей с {hour_start} по {hour_finish}

-- Провести расчет в цикле по часам региона, где будет созданы переменные по именам полей SELECT
FOR SELECT hour
    FROM public.table1
    WHERE regionid = {regionid} AND hour BETWEEN '{hour_start}'::timestamp AND '{hour_finish}'::timestamp;

  UPDATE public.table1
  SET status = 'complete'
  WHERE regionid = {regionid} AND hour = '{hour}'::timestamp
  
  ECHO Час {hour} успешно рассчитан.
END FOR; 
```
P.S. При обработке записей в цикле, оператор _FOR_ сначала получит в память весь массив записей и потом
начнем по нему обработку. Не рекомендуется возвращать в его _SELECT_ большое количество записей.

Пример вызова ресурсного SQL файла:
```groovy
// Соединение к Vertica
def vercon = verticaConnetion {
  connectHost = 'vertica-host1'
  connectDatabase = 'verdb'
  login = 'user'
  password = 'password'
}
// Выполнить SQL скрипт
sql {
  // Использовать указанное соединение
  useConnection vercon

  // Установить значение переменным скрипта
  vars.regionid = 1

  // Выполнить скрипт из ресурсного файла
  runFile 'resource:/sql/script.sql'

  logInfo "Было обработано ${vars.count_rows} записей"
}
```  

# <a name="vertica">Расширения в Getl под Micro Focus Vertica
Getl имеет расширенную поддержку работы с аналитическим хранилищем данных Micro Focus Vertica для своих объектов:
* verticaConnection: подключение и отключение внешних кластеров Vertica для копирования данных таблиц между ними, пересчет статистики таблиц,
  дефрагментация таблиц, запуск встроенной функции Vertica для анализа работы запросов и пересчет статистики по полученным рекомендациям.
* verticaTable: копирование и очистка партиций у таблиц, создание таблиц по шаблону с других таблиц, пересчет статистики и дефрагментация таблицы.

Бывают ситуации, когда требуется произвести перерасчет данных в витринах за период задним числом. Если это делать прямо
на рабочей витрине, то придется столкнуться с множеством проблем: удалить старые данные партицией нельзя,
иначе пользователи просто не увидят старых данных, пока не посчитаются новые и не сохраняться в транзакции. А удалять
с помощью оператора DELETE миллионы или миллиарды записей в Vertica приведет к большому времени выполнения такого оператора
и деградации работы SELECT с этой таблицей, пока не будет выполнен _purge_ после перерасчета витрины. Функция обмена партициями
решает все эти проблемы, при условии, что периоды вписываются в партиции таблицы.

Пример перерасчета аналитической витрины Vertica, в котором в стейджинговой области создается промежуточная таблица и после выполнения перерасчета
производится обмен партициями между основной и промежуточной  таблицей для замены текущих значений на рассчитанные, c дефрагментацией и пересчетом статистики рабочей таблицы
в конце работы:
```groovy
// Соединение к Vertica
useVerticaConnection verticaConnetion {
  connectHost = 'vertica-host1'
  connectDatabase = 'verdb'
  login = 'user'
  password = 'password'
}

// Таблица витрины с суточной партицией и группировкой дней в месяца
verticaTable('marts:dm1', true) {
  schemaName = 'marts'
  tableName = 'dm1'
}

// Промежуточная таблица
verticaTable('#temp_dm1', true) {
  schemaName = 'stage'
  tableName = 'dm1'
  if (!exists)
  // Создать по формату витрины с сохранением партиционирования, проекций и прав доступа
    createLike verticaTable('marts:dm1'), true, true
  else
  // Очистить таблицу
    truncate()
}

// Код расчета периода витрины задним числом в стейджинговой области
callScript CallMart

verticaTable('marts:dm1') {
  // Поменять партиции между двумя таблицами на указанный период перерасчета
  swapPartitionsBetweenTables 'начало периода', 'конец периода', verticaTable('#temp_dm1')
  // Дефрагментация таблицы
  purgeTable()
  // Пересчет статистики таблицы
  analyzeStatistics()
} 
```
P.S. Промежуточная таблица начинается в имени с символа "#", что делает ее временной для Getl. После выполнения скрипта она будет
автоматически удалена из репозитопия.

Пример копирования записей таблицы между кластерами Vertica:
```groovy
// Соединение к приёмнику Vertica
verticaConnection('dest', true) {
  connectHost = 'vertica-stand1'
  connectDatabase = 'db1'
  login = 'user'
  password = 'password'
}

// Соединение к источнику Vertica
verticaConnection('source', true) {
  connectHost = 'vertica-stand2'
  connectDatabase = 'db2'
  login = 'user'
  password = 'password'
}

verticaConnection('dest') {
  // Добавление внешнего соединения источника данных к текущему соединению
  attachExternalVertica verticaConnection('source')
  // Выполнение команды копирования между кластерами
  executeCommand 'COPY public.table1 FROM VERTICA db2.public.table1'
  // Отсоединение источника от текущего соединения
  detachExternalVertica verticaConnection('source')
}
``` 
P.S. Getl запоминает, какие внешние подключения были связаны с соединением Vertica. Не удастся второй раз присоединить тот же
кластер Vertica или разъединить не присоединенный ранее кластер.

Пример дефрагментации таблиц Vertica, у которых высокий процент удаленных записей и пересчета статистики:
```groovy
// Соединение к Vertica
verticaConnection {
  connectHost = 'vertica-host1'
  connectDatabase = 'verdb'
  login = 'user'
  password = 'password'

  // Дефрагментировать все таблицы, у которых более 15% удаленных записей от общего количества
  purgeTables 15
  // Пересчитать статистику всех таблиц
  analyzeStatistics()
}
``` 

Пример работы с анализатором запросов Vertica для выявления устаревшей статистики и её пересчета:
```groovy
// Соединение к Vertica
verticaConnection {
  connectHost = 'vertica-host1'
  connectDatabase = 'verdb'
  login = 'user'
  password = 'password'

  // Получить список рекомендаций анализатора Vertica
  def problems = analyzeWorkload(new Date())
  // Обработать список рекомендаций и выполнить рекомендуемые действия
  processWorkload problems
}
```

# <a name="fileman">Работа с файловыми системами
Getl поддерживает работу со следующими файловыми системами: локальные, FTP, SFTP и HDFS. Можно копировать, перемещать, удалять и парсить
группы файлов по заданным маскам пути и условиям.

Пример работы с файловой системой HDFS:
```groovy
// Менеджер HDFS
hdfs {
  // Параметры подключения
  server = 'hdfs.host'
  login = 'user'
  // Корневой путь сервера
  rootPath = '/users/user/data'
  // Корневой путь локальной файловой системы клиента
  localDirectory = '/temp/files'

  // Создать директорию, если её не обнаружено
  if (!existDirectory('files')) createDir 'files'
  // Перейти в директорию
  changeDirectory 'files'
  // Загрузить файл на сервер с локальной директории
  upload 'file1.txt'
  // Переименовать файл
  rename 'file.txt', 'file2.txt'
  // Выгрузить файл с сервера в локальную директорию
  download 'file2.txt'
  // Перейти в родительскую директорию
  changeDirectoryUp()
  // Удалить директорию с файлом
  removeDir 'files', true
}
```

## <a name="fileman_copy">Копирование файлов
Оператор "_fileman.copier_" позволяет копировать файлы с источника в приёмник по заданным правилам.

Пример захвата CSV файлов с FTP сервера, файлы располагаются по директориям за сутки. Требуется копировать появившиеся новые файлы на локальную файловую
систему и загружать их в таблицу Vertica:
```groovy
// Соединение к Vertica
useVerticaConnection verticaConnetion('ver:con', true) {
  connectHost = 'vertica-host1'
  connectDatabase = 'verdb'
  login = 'user'
  password = 'password'
}

// Таблица для загрузки данных из файлов
verticaTable('data', true) {
  schemaName = 'stage'
  tableName = 'table1'
  assert exists, "В БД не найдена таблица $fullTableName!"
}

// Таблица для хранения истории копирования файлов
verticaTable('history', true) {
  schemaName = 'stage'
  tableName = 's_history_table1'
}

// Определение источника файлов на FTP сервере
ftp('source', true) {
  server = 'ftp.domain'
  rootPath = '/files'
  login = 'user'; password = 'password'
  // Использовать таблицу для хранения истории копирования
  useStory verticaTable('history')
  // Создать таблицу истории, если она отсутствует в БД
  createStory = true
}

// Определение приемника файлов в указанной локальной директории
files('dest', true) {
  rootPath = '/data/files/from_load'
}

// Копирование файлов между файловыми источниками
// P.S. с источника будут скопированы только новые файлы, которых еще нет в таблице истории
fileman.copier(ftp('source'), files('dest')) {
  // Задать маску поиска файлов на источнике
  useSourcePath {
    mask = '{date}/data_{region}.{num}.txt'
    // Определение переменных маски
    variable('date') { type = dateFieldType; format = 'yyyyMMdd' }
    variable('num') { type = integerFieldType; length = 3 }
  }

  // Задать маску директория назначения для копирования файлов на приёмнике
  useDestinationpath {
    mask = '{region}/{date}'
  }

  // При ошибке попробовать повторить 3 раза
  numberAttempts = 3
  // Количество секунд ожидания между повторами при ошибках
  timeAttempts = 2
  // Порядок копирования файлов согласно заданной сортировке из переменных маски исходного файла
  order = ['date', 'num']
}

// Описание CSV файла
csv('bulk_file', true) {
  // Задать путь хранения файлов
  useConnection csvConnection { path = files('dest').rootPath }
  // Задать формат записи CSV файла
  fieldDelimiter = '|'
  codePage = 'utf-8'
  // Назначить для CSV поля из таблицы Vertica, куда будет производиться загрузка CSV файлов
  field = verticaTable('data').field
}

// Обратится к ранее описанной таблице Vertica
verticaTable('data') {
  // Запустить пакетную загрузку файлов в таблицу по параметрам CSV файла
  bulkLoadCsv(csv('bulk_file')) {
    // Задать путь поиска файлов для загрузки
    files = filePath {
      mask = '{date}/data_{region}.{num}.txt'
      // Определить переменные маски
      variable('date') { type = dateFieldType; format = 'yyyyMMdd' }
      variable('num') { type = integerFieldType; length = 3 }
    }

    // Загружать все найденные файлы группой в одном операторе COPY
    loadAsPackage = true
    // Определить порядок загрузки файлов
    orderProcess = ['date', 'num']

    // При обнаружении ошибок записывать в указанный файл
    exceptionPath = csv('bulk_file').currentCsvConnection.path + '/vertica.bulkload.err'
    // При обнаружении некорретных значений полей записей записывать их в указанный файл
    rejectedPath = csv('bulk_file').currentCsvConnection.path + '/vertica.bulkload.err'

    // Удалять файлы после успешной загрузки
    removeFile = true
  }
}
``` 
При вызове сценария, Getl просмотрит на FTP все директории, которые по имени могут быть распарсены в дату,
создаст в локальной директории нужную иерархию директорий, скопирует по ней файлы и одним запросом загрузит все файлы в
Vertica.

P.S. "_fileCopier_" поддерживает множество режимов работы, в том числе зеркальное копирование на несколько источников одновременно или
сегментное копирование на несколько источников по заданному хэш ключу. Последнее позволяет из одного источника взять файлы,
скопировать их веером по кластеру ETL серверов и на каждом запустить свою обработку доставленных данных. Таким способом можно
организовать распределенную обработку файлов.

## <a name="fileman_clean">Очистка файлов
Оператор "_fileman.cleaner_" позволяет удалять файлы в источнике по заданным правилам.

Пример очистки файлов в директориях за сутки старше 3 дней хранения:
```groovy
// Определить источник для очистки файлов
def localfiles = files {
  rootPath = '/tmp/data'
}

// Определить текущую дату
def limitDay = DateUtils.CurrentDate()
// Отнять 3 дня
use(TimeCategory) { limitDay = limitDay - 3.days }

// Выполнить очистку файлов
fileman.cleaner(localfiles) {
  // Задать маску поиска файлов
  useSourcePath {
    mask = '{date}/*'
    // Определение переменных маски
    variable('date') { type = dateFieldType; format = 'yyyyMMdd' }
  }

  // Установить фильтр для директориев источника, в которых будет произведено удаление
  filterDirs { attr ->
    attr.date <= limitDay
  }

  // Удалить все пустые директории
  removeEmptyDirs = true
}
```
При вызове сценария, Getl просмотрит по локальному пути все директории, которые по имени могут быть распарсены в дату, применит заданный фильтр
и удалит все файлы в выявленных директориях. Также рекурсией будут удалены все директории, в которых нет других файлов или директориев.

## <a name="fileman_processing">Парсинг файлов
Оператор "_fileman.processing_" позволяет организовать многопоточный парсинг файлов из файлового источника.

Пример захвата из SFTP источника JSON файлов, в которых реализовано хранение данных в виде master-detail и требуется в один проход эти записи
вставить в таблицы Vertica:
```groovy
// Соединение к Vertica
useVerticaConnection verticaConnetion('ver:con', true) {
  connectHost = 'vertica-host1'
  connectDatabase = 'verdb'
  login = 'user'
  password = 'password'
}

// Таблица для хранения основных данных
verticaTable('master', true) {
  schemaName = 'stage'
  tableName = 'master_table'
  assert exists, "В БД не найдена таблица $fullTableName!"
}

// Таблица для хранения подчиненных данных
verticaTable('detail', true) {
  schemaName = 'stage'
  tableName = 'detail_table'
  assert exists, "В БД не найдена таблица $fullTableName!"
}

// Источник JSON файлов
sftp('source', true) {
  server = 'ftp.domain'
  rootPath = '/files'
  login = 'user'
  password = 'password'
  hostKey = 'ключ RSA'
}

// Описание JSON файла
json('source-file', true) {
  // Элемент, в котором хранится список требуемых для парсинга записей
  rootNode = 'data'
  // Поля файла (по умолчанию имена элементов JSON должны им соответствовать)
  field('id') { type = integerFieldType }
  // Указывает, что при чтении брать значение поля из элемента "_datetime_" в JSON
  field('dt') { type = datetimeFieldType; alias = 'datetime' }
  // Указывает, что поле имеет составной тип и должно быть разобрано в коде парсинга
  field('details') { type = objectFieldType }
}

fileman.processing(sftp('source')) {
  // Маска поиска файлов на источнике
  useSourcePath {
    mask = '{date}/data.{num}.json'
    // Определение переменных маски
    variable('date') { type = dateFieldType; format = 'yyyyMMdd' }
    variable('num') { type = integerFieldType; length = 3 }
  }

  // Удалять успешно обработанные файлы
  removeFiles = true
  // Удалять все пустые директории
  removeEmptyDirs = true
  // Парсить список файлов в 16 потоков
  countOfThreadProcessing = 16
  // Группировать файлы в потоки по заданной переменной, получаемой из имени файла
  threadGroupColumns = ['date']
  // Обрабатывать файлы в указанном порядке по заданной переменной, получаемой из имени файла
  order = ['num']

  // Код обработки файла в потоке
  processFile { inf -> // дескриптор обрабатываемого файла
    logFine "Парсинг файла ${inf.filepath}/${inf.filename} ..."

    // Установить для JSON файла путь и имя файла из дескриптора файла
    json('source-file') {
      currentJSONConnection.path = inf.file.parent
      fileName = inf.file.name
    }

    // Копирование записей из файла JSON в таблицу Vertica
    etl.copyRows(json('source-file'), verticaTable('master')) {
      // Организовать запись в подчиненную таблицу Vertica
      childs(verticaTable('detail')) {
        // Код записи в подчиненную таблицу
        writeRow { add, source_row -> // Дескриптор и обрабатываемая запись
          // Перебрать список записей из поля "details"
          source_row.details?.each { elem ->
            // Записать в подчинненую таблицу id мастер записи и значение из списка детализации
            add master_id: source_row.id, value: elem.value
          }
        }
      }
    }

    // Выполнить проверку для каждой обрабатываемой записи из JSON файла
    copyRow { source_row, dest_row ->
      assert source_row.id != null
    }

    // Установить флаг для подтверждения успешного парсинга файла
    inf.result = inf.completeResult

    logInfo "Из файла ${inf.filepath}/${inf.filename} загружено ${verticaTable('master').updateRows} " +
            "записей в master и ${verticaTable('detail')} записей в detail."
  }
}
```
При запуске сценария с SFTP сервера будет получен список подходящих файлов. Файлы будут сгруппированы по датам и для каждой даты по очереди
в порядке возрастания будет произведен парсинг подходящих файлов в 16 потоков. В ходе обработки каждого файла будет произведен парсинг из JSON в записи,
которые будут записаны в таблицы Vertica. Для корректной работы в потоках Getl будет автоматически для каждого обрабатываемого файла клонировать соединение и таблицы Vertica
и закрывать их после завершения обработки файла. Таким образом, в ходе работы кода к Vertica будет открыто 17 сессий (1 главная и 16 потоков). Запись в главную и подчиненную таблицы
Vertica будет производиться в рамках единой транзакции. После успешного завершения обработки файлов они будут удаляться на источнике, при ошибках оставаться на источнике. После окончания
обработки всех файлов будут удалены все пустые директории.

P.S. В зависимости от того, какой результат указан в "_result_" у дескриптора обработки файла, Getl будет производить разный набор действий:
* Для "_completeResult_" файл будет считаться успешно обработанным. Он будет перенесен в файловую систему для хранения обработанных файлов,
  если она задана в параметре "_storageProcessedFiles_". Если выставлен флаг для "_removeFiles_", то файл будет удален с источника;
* для "_errorResult_" файл будет считаться ошибочным. Он будет перенесен в файловую систему для хранения ошибочных файлов,
  если она задана в параметре "_storageErrorFiles_". Вместе с файлом рядом будет создан текстовый файл с описанием ошибки и
  трассировкой его выполнения на момент ошибки. Если выставлен флаг "_removeFiles_" и задано место для хранения ошибочных файлов,
  то файл будет удален с источника;
* Для "_ignoreResult_" файл будет считаться игнорируемым. Он останется на источнике.

# <a name="devops">Работа со стендами
С помощью конфигураций в Getl можно упростить разработку, тестирование и работу проекта на разных стендах. В Getl
поддерживается GroovySlurper формат конфигураций. Фактически это Groovy DSL язык конфигураций, в котором можно
указывать не только значения, но и Groovy выражения:
```groovy
раздел {
  массивы {
    a = [1,2,3]
    b = [4,5]
    c = a + b
  }
  d = new Date()
  e = 'Текущая дата'
  f = "$e: $d"
}
```

С помощью ключевого слова _environments_ в конфигурации можно разделить хранения информации по разным средам. Пример
конфигурации подключения к дев и прод стенду Vertica, сохраненной в ресурсном файле "_/data/vertica.connect.conf_":
```groovy
// Ключевое слово Groovy для разделения сред хранения конфигурации
environments {
  // Среда dev (по умолчанию используется при работе unit-тестах)
  dev {
    // Конфигурация для соединений
    connections {
      // Конфигурация con1
      con1 {
        connectHost = 'vertica-dev-host1'
        connectDatabase = 'verdb'
        extended { backupservernode = 'vertica-dev-host2,vertica-dev-host3' }
        login = 'user'
        password = 'password'
      }
    }
  }

  // Среда prod (по умолчанию используется при работе из командной строки Java)
  prod {
    // Конфигурация для соединений
    connections {
      // Конфигурация con1
      con1 {
        connectHost = 'vertica-prod-host1'
        connectDatabase = 'verdb'
        extended { backupservernode = 'vertica-prod-host2,vertica-prod-host3' }
        login = 'user'
        password = 'password'
      }
    }
  }
}
```
P.S. Для соединений параметры нужно описывать в разделе _connections_ конфигураций, для файловых источников в разделе _files_.

Пример загрузки параметров для соединения Vertica из конфигурационного файла:
```groovy
package data

import groovy.transform.BaseScript
import getl.lang.Getl

@BaseScript Getl main

configuration {
  // Загрузка конфигурации под текущей средой выполнения
  load 'resource:/data/vertica.connect.conf'
}

// Соединение к Vertica
verticaConnection('ver:con', true) {
  // Подгрузить значения параметров соединения из секции конфигурации "con1"
  useConfig 'con1'
}
```
* При вызове этого сценария из командной строки, он по умолчанию будет работать в "_prod_" среде. При чтении файла конфигурации
  будет читаться раздел "_environments.prod_";
* При вызове этого сценария из-под unit test класса в IDE, он по умолчанию будет работать в "_dev_" среде и подключаться
  к стенду разработки;
* Можно явно указать при вызове сценария, в какой среде работать, с помощью параметра командной строки "_environment=<среда>_".

Для процессов работы с JDBC источниками часто бывает необходимо использовать разные логины в ходе работы сценариев.
Для этого можно описать логины и пароли пользователей в раздельном разделе конфигурации. Пример конфигурации с разными логинами
в ресурсном файле "_/data/vertica.logins.conf_":
```groovy
logins {
  vertica {
    user1 = 'password1'
    user2 = 'password2'
  }
}
```

Пример загрузки логинов из конфигурации в соединение Vertica:
```groovy
package data

import groovy.transform.BaseScript
import getl.lang.Getl

@BaseScript Getl main

configuration {
  // Загрузить конфигурацию параметров подключения к Vertica
  load 'resource:/data/vertica.connect.conf'
  // Загрузить конфигурацию списка логинов Vertica
  load 'resource:/data/vertica.logins.conf'
}

// Соединение к Vertica
verticaConnection('ver:con', true) {
  // Подгрузить значения параметров соединения из секции конфигурации "con1"
  useConfig 'con1'
  // Установить список доступных логинов из секции конфигурации "logins.vertica"
  storedLogins = configContent.logins.vertica

  // Переключение соединения на user1
  useLogin 'user1'
  // Переключение соединения на user2
  useLogin 'user2'
}
```
P.S. При переключении между логинами текущее соединение будет разорвано и создано под новым пользователем.

# <a name="unit_tests">Разработка unit-тестов
Для разработки unit-тестов в Getl есть специальный класс "_GetlDslTest_". Рекомендуется наследовать классы тестирования от этого класса, чтобы
гарантировать корректную работу Getl в режиме тестирования. Класс обеспечивает:
* Сброс настроек, конфигурации и репозитория Getl перед выполнением каждого unit-теста;
* Установку среды выполнения "_dev_" для конфигураций и репозитория;
* Возможность задать собственный класс запуска Dsl скриптов и класс инициализации перед запуском, наследуемые от Getl;
* Возможность переопределить метод "_allowTests_" для определения разрешения или запрета проведения тестов класса;
* Расширение метода "_assertEquals _" для поддержки сравнения Duration объектов и сравнения объектов Map.

Пример класса тестирования сценария proc.Proc1 с очисткой таблиц перед выполнением тестируемого скрипта и проверкой их заполнения после выполнения
тестируемого скрипта:
```groovy
package proc

import getl.test.GetlDslTest
import getl.lang.Getl
import org.junit.Test

// Наследование от базового класса тестирования Getl
class Proc1Test extends GetlDslTest {
  @Test
  void testProc1() {
    // Выполнение Getl операторов
    Getl.Dsl {
      // Вызвать скрипт описания подключения к Vertica
      callScripts data.ConnectToVertica

      // Обработать список таблиц Vertica из схемы "demo"
      verticaConnection('ver:con').retrieveDatasets(schemaName: 'demo').each { table ->
        // Очистить таблицу из списка
        (table as VerticaTable).truncate()
      }

      // Вызывать тестируемый скрипт
      callScript proc.Proc1

      // Обработать список таблиц Vertica из схемы "demo"
      verticaConnection('ver:con').retrieveDatasets(schemaName: 'demo').each { table ->
        // Проверить, что в таблицах есть записи
        assertTrue((table as VerticaTable).countRows() > 0)
      }
    }
  }
} 
```
P.S. Если требуется запустить метод unit-теста под другой средой выполнения, ему нужно указать аннотацию @Config(env='<среда>'). Это позволяет сделать тесты для
запуска на разных тестовых средах или при должной осторожности на промышленной среде выполнения.

# <a name="models">Модели Getl
В большинстве случаев при разработке ETL задач приходится оперировать группами объектов. Например, скопировать данные из списка таблиц источника и загрузить
в таблицы приёмника, переместить группу файлов на другой источник и т.д. В Getl поддерживаются различные модели группировки объектов, которые позволяют описать набор объектов и правила
работы с ними.

## <a name="model_ref_files">Модель эталонных файлов referensFiles
Позволяет автоматизировать доставку и распаковку архивов эталонных файлов в указанное место. С помощью этой модели легко автоматизировать подготовку тестирования Etl процессов, которые
копируют или обрабатывают файлы. В описании модели указываются два файловых менеджера, источник и приемник, а также перечисляются архивы, которые содержат эталонные файлы.

Пример хранения двух архивов с эталонными файлами для референсной модели "_model1_" в скрипте "_ref.Model1_":
```groovy
package ref

import groovy.transform.BaseScript
import getl.lang.Getl

@BaseScript Getl main

// Описание источника на FTP сервере
ftp('source', true) {
  server = 'host'
  user = 'user'
  password = 'password'
  rootPath = '/reference_files'
}

// Описание приёмника в локальной директории
files('dest', true) {
  rootPath = '/files_for_loading'
}


// Описание модели эталонных файлов
models.referensFiles('model1', true) {
  // Указание источника
  useSourceManager 'source'
  // Указание приёмника
  useDestinationManager 'dest'
  // Задать команду распаковки архива после его доставки на приёмник
  unpackCommand = '7z x -y -bd "{file}"'

  // Определить эталонные файлы источника и пути их доставки на приёмник
  referenceFromFile('file1.7z') { destinationPath = 'dir1' }
  referenceFromFile('file2.7z') { destinationPath = 'dir2' }
}
```
P.S. В команде распаковки 7z задана опция развертывания файлов с сохранением структуры, поэтому в архивах можно сохранить директории и файлы, нужные для тестирования
процессов.

Пример использования модели в unit-тестах:
```groovy
package proc

import getl.test.GetlDslTest
import getl.lang.Getl
import org.junit.Test
import ref.Model1

class Proc2Test extends GetlDslTest {
  @Test
  void testProc2() {
    Getl.Dsl {
      // Вызвать скрипт описания модели эталонных файлов
      callScripts Model1
      // Доставить файлы в приёмник и развернуть их там
      models.referensFiles('model1') { fill() }
      // Запустить тестируемый процесс
      callScript proc.Proc2
      // ... Проверка результатов работы процесса ...
    }
  }
} 
```

## <a name="model_ref_ver_tables">Модель эталонных данных Vertica referenceVerticaTables
Позволяет автоматизировать хранение и копирование эталонных данных для таблиц Vertica. С помощью этой модели легко автоматизировать подготовку тестирования ETL процессов, которые
работают с таблицами Vertica. В описании модели указывается соединение к Vertica, имя схемы БД, в которой будут хранится эталонные данные и перечисляются таблицы Vertica,
для которых нужно хранить эталонные данные.

Пример хранения эталонных данных таблиц для референсной модели "_model2_" в скрипте "_ref.Model2_":
```groovy
package ref

import groovy.transform.BaseScript
import getl.lang.Getl

@BaseScript Getl main

// Соединение к Vertica
useVerticaConnection verticaConnection('ver', true) {
  connectHost = 'host1'
  connectDatabase = 'db'
  login = 'user'
  password = 'password'
}

// Описание таблиц Vertica, для которых будут сохранены эталонные данные
verticaTable('ver:dim1', true) {
  schemaName = 'public'
  tableName = 'dim1'
}

verticaTable('ver:dim2', true) {
  schemaName = 'public'
  tableName = 'dim2'
}

verticaTable('ver:fact', true) {
  schemaName = 'public'
  tableName = 'fact'
}

verticaTable('ver:mart', true) {
  schemaName = 'public'
  tableName = 'mart'
}

// Описание модели эталонных данных таблиц Vertica
models.referenceVerticaTables('model2', true) {
  // Использовать соединение Vertica
  useReferenceConnection 'ver'
  // Сохранить эталонные данные в заданной схеме БД Vertica
  referenceSchemaName = '_ref_model2'

  // Переменные модели для использования в SQL скриптах
  modelVars.point = DateUtils.ParseDate('2020-01-01')

  // Сохранить все записи таблицы в эталонные данные
  referenceFromTable('ver:dim1') { allowCopy = true }
  // Сохранить записи таблицы по заданным условиям в эталонные данные
  referenceFromTable('ver:dim2') { whereCopy = "'{point}'::date BETWEEN start_date AND finish_date" }
  // Сохранить записи таблицы по заданным условиям с сэмплированием 10% в эталонные данные
  referenceFromTable('ver:fact') {
    whereCopy = "fact_date <= '{point}'::date"
    sampleCopy = 10
  }
  // Не сохранять для таблицы эталонные данные и считать эталоном пустую таблицу
  referenceFromTable('ver:mart')
}
```

Пример создания схемы хранения и таблиц эталонных данных:
```groovy
models.referenceVerticaTables('model2') { createReferenceTables() }
```
P.S. Если таблицы уже присутствуют в БД, то они будут пропущены и не будут пересоздаваться. Для пересоздания таблиц при вызове метода
"_createReferenceTables_" требуется указать в параметре "_recreate"_ значение "_true_".

Пример сохранения эталонных данных из исходных таблиц:
```groovy
models.referenceVerticaTables('model2') { copyFromSourceTables() }
```
P.S. Если в эталонной таблице уже ранее были сохранены записи, то в нее не будут копироваться записи. Для перезаписи эталонных
данных при вызове метода "_copyFromSourceTables_" требуется указать в параметре "_onlyForEmpty_" значение "_false_".

Пример сохранения эталонных данных из таблиц другого кластера Vertica:
```groovy
// Соединение к внешнему кластеру Vertica
def extver = verticaConnection {
  connectHost = 'prod-host1'
  connectDatabase = 'db'
  user = 'user'
  password = 'password'
}

models.referenceVerticaTables('model2') { copyFromVertica(extver) }
```
P.S. Если в эталонной таблице уже ранее были сохранены записи, то в нее не будут копироваться записи. Для перезаписи эталонных
данных при вызове метода "_copyFromVertica_" требуется указать вторым параметром "_onlyForEmpty_" значение "_false_".

Пример использования эталонных данных в unit-тестах:
```groovy
package proc

import getl.test.GetlDslTest
import getl.lang.Getl
import org.junit.Test
import ref.Model2

class Proc3Test extends GetlDslTest {
  @Test
  void testProc2() {
    Getl.Dsl {
      // Вызвать скрипт описания модели эталонных данных
      callScripts Model2
      // Заполнить исходные таблицы данными из сохраненных в эталонных таблицах
      models.referensVerticaTables('model2') { fill() }
      // Вызывать тестируемый процесс
      callScript proc.Proc3
      // ... Проверка результатов работы процесса ...
    }
  }
} 
```
P.S. Если в исходных таблицах находится такое же количество записей, что и в эталонных таблицах, то в них не происходит заново копирование эталонных данных.

## <a name="model_set_tables">Модель набора таблиц setOfTables
Позволяет описать набор таблиц, которые можно использовать в процессах или шаблонах, если требуется обработать более одной таблицы. В описании модели указывается соединение и
перечисляются таблицы этого соединения.

Пример группировки таблиц для референсной модели "_model3_" в скрипте "_sets.Model3_":
```groovy
package sets

import groovy.transform.BaseScript
import getl.lang.Getl
import getl.utils.DateUtils

@BaseScript Getl main

// Соединение к Vertica, назначаемое таблицам Vertica
useVerticaConnection verticaConnection('ver', true) {
  connectHost = 'host1'
  connectDatabase = 'db'
  login = 'user'
  password = 'password'
}


// Описание исходных таблиц
verticaTable('ver:table1', true) {
  schemaName = 'public'
  tableName = 'table1'
}

verticaTable('ver:table2', true) {
  schemaName = 'public'
  tableName = 'table2'
}

// Описание модели группировки таблиц 
models.setOfTables('model3', true) {
  // Использование соединения
  useSourceConnection 'ver'

  // Переменнные модели
  modelVars.delete_date = DateUtils.CurrentDate()

  // Добавить в модель таблицу с атрибутом
  table('ver:table1') {
    attrs.method = 'TRUNCATE'
  }

  // Добавить в модель таблицу с 2 атрибутами
  table('ver:table2') {
    attrs.method = 'DELETE'
    attrs.where = "DT <= '{delete_date}'::date"
  }
}
```

Пример шаблона "_patterns.CleanTables_", задача которого очищать список таблиц, взятый из модели, по заданным правилам:
```groovy
package patterns

import groovy.transform.BaseScript
import getl.lang.Getl
import groovy.transform.Field
import getl.models.SetOfTables
import static getl.utils.StringUtils.WithGroupSeparator

@BaseScript Getl main

// Обрабатываемая модель группы таблиц
@Field SetOfTables tables

// Проверка перед запуском логики шаблона
void check() {
  assert tables != null, 'Список таблиц не указан!'
  // Проверка имен атрибутов таблиц модели
  tables.checkAttrs ['method', 'where']
}

logFine "Очистка ${tables.usedTables.size()} таблиц ..."

// Обработка таблиц модели
tables.usedTables.each { node ->
  def table = node.sourceTable
  def method = (node.attrs.method as String)?.toUpperCase()
  def where = node.attrs.where as String

  assert method in ['TRUNCATE', 'DELETE'], "Неизвестный метод очистки \"$method\""

  // Очистка таблицы нужным способом
  if (method == 'TRUNCATE') {
    table.truncate()
    logInfo "Таблица \"$table\" успешно очищена"
  }
  else {
    def count = table.deleteRows(where)
    logInfo "В таблице \"$table\" успешно удалено ${WithGroupSeparator(count)} записей"
  }
}
```
P.S. В шаблоне разрешено использовать два атрибута для таблиц модели: "_method_" и "_where_". С помощью метода "_checkAttrs_" у модели вызывается проверка атрибутов таблиц на эти значения.
Если найдены неизвестные атрибуты, будет сгенерирована ошибка.

Пример использования шаблона в unit-тесте:
```groovy
package patterns

import getl.test.GetlDslTest
import getl.lang.Getl
import org.junit.Test
import sets.Model3

class CleanTablesTest extends GetlDslTest {
  @Test
  void testCleanTables() {
    Getl.Dsl {
      // Вызвать скрипт описания модели
      callScripts Model3
      // Вызывать шаблон
      callScript new CleanTables(tables: models.setOfTables('model3'))
      // Проверить по таблицам модели, что они все очищены
      models.setOfTables('model3').usedTables.each { node ->
        assertEquals(0, node.sourceTable.countRow())
      }
    }
  }
} 
```

## <a name="model_map_tables">Модель маппинга между двумя источниками mapTables
Позволяет описать маппинг таблиц между источником и приёмником. В описании модели указываются соединения источника и приёмника и связи между таблицами этих источников.

Пример описания маппинга копирования из таблиц Oracle в таблицы Vertica для модели "_model4_" в скрипте "_maps.Model4_":
```groovy
package maps

import groovy.transform.BaseScript
import getl.lang.Getl

@BaseScript Getl main

// Соединение к Oracle
useOracleConnection oracleConnection('ora', true) {
  connectHost = 'host1'
  connectDatabase = 'db'
  login = 'user'
  password = 'password'
}

// Таблицы Oracle
oracleTable('ora:table1', true) {
  schemaName = 'user'
  tableName = 'table1'
}

oracleTable('ora:table2', true) {
  schemaName = 'user'
  tableName = 'table2'
}

// Соединение к Vertica
useVerticaConnection verticaConnection('ver', true) {
  connectHost = 'host2'
  connectDatabase = 'db'
  login = 'user'
  password = 'password'
}

// Таблицы Vertica
verticaTable('ver:table1', true) {
  schemaName = 'public'
  tableName = 'table1'
}

verticaTable('ver:table2', true) {
  schemaName = 'public'
  tableName = 'table2'
  writeOpts { batchSize = 10000 }
}

// Описание модели маппинга
models.mapTables('model4', true) {
  // Задать соединение к источнику Oracle
  useSourceConnection 'ora'
  // Задать соединение к приёмнику Vertica
  useDestinationConnection 'ver'

  // Копирование всех записей Oracle таблицы table1 в Vertica таблицу table1
  mapTables('ora:table1') {
    linkTo 'ver:table1'
    // Атрибуты копирования
    attrs.hints = 'PARALLEL (10)'
    attrs.where = 'field1 IS NOT NULL'
  }

  // Копирование записей заданных партиций Oracle таблицы table2 в Vertica таблицу table2
  mapTables('ora:table2') {
    linkTo 'ver:table2'
    // Записывать в поле "fact_date" таблицы Vertica значение из поля "dt" таблицы Oracle
    map.fact_date = 'dt'
    // Список копируемых партиций таблицы Oracle
    listPartitions = ['2020-01-01', '2020-01-02', '2020-01-03']
  }
}
```

Пример шаблона "_CopyOracleToVertica_", задача которого копировать данные из таблиц Oracle в таблицы Vertica по заданным правилам:
```groovy
package patterns

import groovy.transform.BaseScript
import getl.lang.Getl
import groovy.transform.Field
import getl.models.MapTables
import static getl.utils.StringUtils.WithGroupSeparator

@BaseScript Getl main

// Обрабатываемая модель маппинга таблиц
@Field MapTables maps
// Количество потоков копирования таблиц
@Field Integer countThreads = 1

// Проверка параметров шаблона
void check() {
  assert maps != null && !maps.isEmpty(), 'Маппинг таблиц не указан!'
  assert countThreads?:0 > 0, 'Количество потоков должно быть больше нуля!'
  // Проверить имена атрибутов таблиц маппинга
  tables.checkAttrs ['hints', 'where']
}

logFine "Копирование ${tables.usedTables.size()} таблиц из Oracle в Vertica ..."

// Многопоточная обработка
thread {
  // Использовать для обработки список маппинга таблиц
  useList usedMapping
  // Установить количество потоков выполнения
  setCountProc countThreads
  // Остановить работу при ошибке в любом из потоков
  abortOnError = true
  // Обработка правила маппинга в потоке
  run { MapTableSpec node -> // Описание маппинга таблиц
    def source = node.source as OracleTable
    def dest = node.destination as VerticaTable
    def node_maps = node.map

    // Установить параметры чтения Oracle таблицы
    source.readOpts {
      if (node.attrs.hints != null)
        hints = node.attrs.hints
      if (node.attrs.where != null)
        where = node.attrs.where
    }

    // Получить список партиций для обработки таблицы или взять константу, если партиционирования нет
    def isPartition = !node.listPartitions.isEmpty()
    def listPartition = (isPartition)?node.listPartitions:[0]

    // Обработка списка партиций
    listPartition.each { part ->
      if (isPartition)
      // Установить значение партиции для чтения только ёё записей из таблицы Oracle
        source.readOpts { usePartition = part }

      // Копирование записей из таблицы Oracle в Vertica с заданным маппингом полей
      etl.copyRows(source, dest) { it.maps = node_maps }

      logInfo "Из таблицы \"$source\" прочитано ${WithGroupSeparator(source.readRows)} записей, в таблицу \"$dest\" записано ${WithGroupSeparator(dest.updateRows)} записей"
    }
  }
}
logInfo "Копирование завершено."
```
P.S. В шаблоне разрешено использовать два атрибута параметров таблиц: "_hints_" и "_where_". С помощью метода "_checkAttrs_" у модели вызывается проверка атрибутов таблиц на эти значения.
Если заданы неизвестные атрибуты, будет возвращена ошибка.

Пример использования модели в unit-тесте:
```groovy
package patterns

import getl.test.GetlDslTest
import getl.lang.Getl
import org.junit.Test
import sets.Model4

class CopyOracleToVerticaTest extends GetlDslTest {
  @Test
  void testCopyOracleToVertica() {
    Getl.Dsl {
      // Вызвать скрипт описания модели маппинга
      callScripts Model4
      // Вызывать шаблон
      callScript new CopyOracleToVertica(tables: models.mapTables('model4'), countThreads: 2)
      // Проверить, что количество записей у таблиц Oracle и Vertica совпадает после копирования
      models.mapTables('model4').usingMapping.each { node ->
        assertEquals(node.source.countRow(), node.destination.countRow())
      }
    }
  }
} 
```