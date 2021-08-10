# Содержание
* [Объекты языка](#objects)
* [JDBC соединения](#connections)
  * [Описание соединения](#declare_connection)
  * [Управление транзакциями соединений БД](#tran)
* [Датасеты РСУБД](#jdbc_datasets)
  * [Описание таблиц и представлений](#declare_tables)
  * [Создание и удаление таблиц](#ddl_tables)
  * [Описание запросов](#declare_queries)
  * [Получение списка полей таблиц](#fields)
  * [Удаление записей в таблицах БД](#deleting)
  * [Выполнение запросов к таблицам БД](#queries)
  * [Чтение данных из таблиц](#reads)
  * [Запись данных в таблицу БД](#writes)
  * [Копирование записей таблицы в другой источник](#copy)
  * [Пакетная загрузка CSV файлов в таблицу](#bulkload)
  * [Копирование записей в другую таблицу БД](#copytable)
* [Выполнение пакетных сценариев SQL](#sql)
  * [Выполнение текста SQL скрипта](#sql_exec)
  * [Работа с переменными в скриптах](#sql_vars)
  * [Выполнение SQL скрипта из файла](#sql_file)

# <a name="objects"></a>Объекты языка
В Getl поддерживается шесть типов объектов для работы с таблицами РСУБД:
* jdbcConnection: вендорные соединения на для работы с базой данных РСУБД
* table: вендорные датасеты для работы с таблицами БД
* view: датасеты для работы с представлениями БД
* query: датасеты для работы с SQL запросами
* embeddedConnection: соединение к встроенной в Getl БД
* embeddedTable: датасет для работы с таблицами встроенной в Getl БД

## Встроенная в Getl БД
Для хранения и обработки промежуточных данных можно использовать встроенную в Getl БД, реализованную на базе
H2 Database РСУБД. По умолчанию при старте приложения база доступна и располагается в памяти, работая в режиме inmemory.
Для описания датасетов таблиц встроенной БД не обязательно указывать соединение, по умолчанию будет использовано общее 
соединение Getl для работы с этой БД.  При необходимости можно зарегистрировать собственное соединение, задав для него 
хранение данных в файле БД по указанному пути.

# <a name="connections"></a>JDBC соединения
Работа с РСУБД источниками осуществляется с помощью соединений через JDBC драйвера вендоров серверов. Для этого требуется,
чтобы нужные файлы JDBC драйвера вендора (jar файлы и прочие файлы) были доступны в class path запускаемого приложения.
Для запуска скриптов и юнит тестов из под Gradle рекомендуется собрать все файлы JDBC драйверов в директории проекта jdbc
и указать его компиляцию при сборке в разделе dependencies:
```groovy
dependencies {
  compile fileTree(dir: 'jdbc')
}
```

При запуске приложения из командной строки необходимо в -cp параметр включить путь к директорию, где находятся нужные 
файлы JDBC драйверов:
```shell
REM Для Windows
java -cp путь/groovy/*;путь/jdbc/*;путь/проект.jar runClass=my.process.Задача

REM Для Linux
java -cp путь/groovy/*:путь/jdbc/*:путь/проект.jar runClass=my.process.Задача
```

Если для соединения требуется использовать драйвер JDBC другой версии, чем задано в class path, то для соединения можно
явно задать путь к файлу драйвера в свойстве driverPath.

## <a name="declare_connection"></a>Описание соединения
Используйте для работы с РСУБД подходящее соединение:
* db2Connection: соединение к IBM DB2 серверу
* firebirdConnection: соединение к Firebird серверу
* h2Connection: соединение к H2 database серверу или локальной базе
* hiveConnection: соединение к Hadoop Hive серверу
* impalaConnection: соединение к Cloudera Impala серверу
* mssqlConnection: соединение к MS SQLServer серверу
* mysqlConnection: соединение к Mysql серверу
* netezzaConnection: соединение к IBM Netezza серверу
* netsuiteConnection: соединение к NetSuite облаку
* oracleConnection: соединение к Oracle серверу
* postgresqlConnection: соединение к PostgreSql серверу
* verticaConnection: соединение к Micro Focus Vertica серверу

Для соединения с JDBC совместимым РСУБД источником требуется указать его хост в connectHost, имя базы данных 
в connectDatabase, логин в login и пароль в password. Если сервер использует другой порт, отличный от порта по умолчанию, 
он указывается через двоеточие в хосте. Также вместо указания хоста и имени БД возможно указать полный URL для 
подключения к JDBC источнику в свойстве connectURL.

Если объекты соединения будут находится в определенной БД и схеме, для соединения можно указать их имя в свойствах 
dbName и schemaName. В таком случае все объявляемые таблицы, если у них явно не задано имя БД и схема, будут получать
эти свойства, указанные в соединении.

Пример описания соединений:
```groovy
// Регистрируем в репозитории соединение mssql:con1 к БД MSSQL
mssqlConnection('mssql:con1', true) {
  connectHost = 'host1'
  connectDatabase = 'master'
  login = 'user'
  password = 'password'
  // По умолчанию таблицы находятся в БД db1
  dbName = 'db1'
  // По умолчанию таблицы находятся в схеме schema1
  schemaName = 'schema1'
}

// Регистрируем в репозитории соединение ora:con1 к БД Oracle
oracleConnection('ora:con1', true) {
  connectHost = 'host1'
  connectDatabase = 'db1'
  login = 'user'
  password = 'password'
}

// Регистрируем в репозитории соединение ver:con1 к БД Vertica
verticaConnection('ver:con1', true) {
  // Хост указан с портом
  connectHost = 'host1:5433'
  connectDatabase = 'db1'
  login = 'user'
  password = 'password'
}

// Регистрируем в репозитории соединение h2:con1 к БД H2
h2Connection('h2:con1', true) {
  // Указан путь к файлу драйвера H2 с использованием переменной ОС JDBC_PATH
  driverPath = '{JDBC_PATH}/h2-1.4.200.jar'
  // Указан URL для подключения к БД
  connectURL = 'jdbc:h2:tcp://localhost/db1'
  login = 'user'
  password = 'password'
}
```

## <a name="tran"></a>Управление транзакциями соединений БД
Если не задается явное управление транзакциями, каждая операция Getl стартует в неявной транзакции, которая фиксируется
при успешном завершении работы и откатывается при возникновении ошибок. Используйте явные операторы управления транзакциями,
если это требуется в рамках логики работы с источником РСУБД.

Свойства и функции управления транзакциями в соединениях:
* isSupportTran: признак поддержки соединением работы с транзакциями.
* transaction: выполняет код Closure в рамках одной транзакции. После успешного выполнении кода автоматически вызывается
  commit, при любой ошибке, возникшей в коде автоматически вызывается rollback.
* startTran: стартует транзакцию, разрешается вложение вызовов startTran, каждые вложенный вызов увеличивает счетчик
  транзакций tranCount.
* isTran: признак, что соединение в режиме транзакции.
* tranCount: количество вложенных транзакций, вызванных с помощью startTran
* commitTran: подтвердить транзакцию. Уменьшает счетчик вложенных транзакций tranCount на 1, если он доходит до нуля,
  то у соединения вызывается оператор commit.
* rollbackTran: откатить транзакцию.  Уменьшает счетчик вложенных транзакций tranCount на 1, если он доходит до нуля,
  то у соединения вызывается оператор rollback.

Пример работы транзакций:
```groovy
verticaConnection('ver:con1') {
  // Выполнить код внутри единой транзакции соединения
  transaction {
    verticaTable('ver:table1').deleteRows()
    etl.copyRows(mssqlTable('mssql:table1'), verticaTable('ver:table1'))
  }
  
  // Старт транзакции
  startTran()
  try {
    // Выполнить команды в транзакции
    executeCommand 'DELETE FROM schema1.table1'
    etl.copyRows(oracleTable('ora:table1'), verticaTable('ver:table1'))
  }
  catch (Exception e) {
    // Откатить транзакцию
    rollbackTran()
    throw e
  }
  // Подтвердить транзакцию
  commitTran()
}
```

# <a name="jdbc_datasets"></a>Датасеты РСУБД
Для работы с JDBC соединениями можно использовать таблицы, представления и запросы. К каждому из этих датасетов можно
выполнять запросы для чтения данных. В таблицы можно производить запись данных и загрузку CSV файлов (только для 
h2, Hive, Impala и Vertica).

## <a name="declare_tables"></a>Описание таблиц и представлений
Используйте для работы с табличными объектами РСУБД подходящий датасет:
* db2Table: таблица БД IBM DB2
* firebirdTable: таблица БД Firebird
* h2Table: таблица БД H2 database
* hiveTable: таблица БД Hadoop Hive
* impalaTable: таблица БД Cloudera Impala
* mssqlTable: таблица БД MS SQLServer
* mysqlTable: таблица БД Mysql
* netezzaTable: таблица БД IBM Netezza
* netsuiteTable: таблица БД Netsuite
* oracleTable: таблица БД Oracle
* postgresqlTable: таблица БД PostgreSql
* verticaTable: таблица БД Micro Focus Vertica
* view: представление БД

Для табличных объектов должно быть указано имя таблицы или представления в свойстве tableName. Если датасет находится 
в схеме БД, отличной от схемы по умолчанию, требуется указать схему хранения в свойстве schemaName. Аналогично, если 
датасет находится в БД, отличной от умолчания, требуется задать имя БД в свойстве databaseName.

Пример описания табличных объектов:
```groovy
// Регистрируем таблицу mssql:table1 из БД MSSQL
mssqlTable('mssql:table1', true) {
  // Соединение к MSSQL
  useConnection mssqlConnection('mssql:con1')
  // Имя БД, где хранится таблица
  dbName = 'db1'

  // Имя схемы, где хранится таблица
  schemaName = 'schema1'
  // Имя таблицы
  tableName = 'table1'

  // Поля датасета, при работе в Getl приводятся к нижнему регистру
  field('field1') { type = stringFieldType; length = 50; isNull = false }
  field('field2') { type = integerFieldType; isKey = true }
  field('field3') { type = dateFieldType; isNull = false }
  field('field4') { type = datetimeFieldType }
  field('field5') { type = numericFieldType; length = 12; precision = 2 }
}

// Регистрируем таблицу ora:table1 из БД Oracle
oracleTable('ora:table1', true) {
  useConnection oracleConnection('ora:con1')
  schemaName = 'schema1'
  tableName = 'table1'
}

// Регистрируем таблицу ver:table1 из БД Vertica
verticaTable('ver:table1', true) {
  useConnection verticaConnection('ver:con1')
  schemaName = 'schema1'
  tableName = 'table1'

  field('field1') { type = stringFieldType; length = 50; isNull = false }
  field('field2') { type = integerFieldType; isKey = true }
  field('field3') { type = dateFieldType; isNull = false }
  field('field4') { type = datetimeFieldType }
  field('field5') { type = numericFieldType; length = 12; precision = 2 }
}

// Регистрируем представление mssql:view1 из БД MSSQL
view('mssql:view1', true) {
  // Соединение к MSSQL
  useConnection mssqlConnection('mssql:con1')
  // Имя схемы таблицы в БД
  schemaName = 'schema1'
  // Имя представления в БД
  tableName = 'view1'
}
```

## <a name="ddl_tables"></a>Создание и удаление таблиц
За исключение источника NetSuite, все JDBC источники поддерживают создание и удаление таблиц в БД, в опциях создания и 
удаления таблиц можно задать дополнительные параметры, которые зависят от вендора источника:
```groovy
mssqlTable('mssql:table1') {
  // Опции создания таблицы
  createOpts {
    // Создать если не существует
    ifNotExists = true
    
    // Описание индекса для таблицы
    index('idx_table1_1') {
      // Обеспечить уникальность индекса
      unique = true
      // Поля в индексе
      columns = ['field1', 'field2']
      // Создать если не существует
      ifNotExists = true
    }
  }

  // Опции удаления таблицы
  dropOpts {
    // Удалять если существует
    ifExists = true
  }

  // Удалить таблицу
  drop()
  // Создать таблицу
  create()

  // Проверить, что таблица существует
  assert exists
}

verticaTable('ver:table1') {
  // Опции создания таблицы
  createOpts {
      // Задать ключ партиционирования таблицы
      partitionBy = 'Year(field3) * 100 + Month(field3)'
      // Задать сортировку супер проекции
      orderBy = ['field1', 'field2']
      // Задать сегментирование супер проекции
      segmentedBy = 'Hash(\'field1\') ALL NODES'
      // Создать если не существует
      ifNotExists = true
  }
  
  // Опции удаления таблицы
  dropOpts {
      // Удалять если существует
      ifExists = true
  }
  
  // Удалить таблицу
  drop()
  // Создать таблицу
  create()
  
  // Проверить, что таблица существует
  assert exists
}
```

## <a name="declare_queries"></a>Описание запросов
Для работы с запросами требуется задать SQL запрос в свойстве query. Если текст запроса хранится в файле, то в свойстве
scriptFilePath можно задать путь к файлу скрипта, а в свойстве scriptFileCodePage задать кодировку текста файла, если она
не совпадает с utf-8.

В тексте запроса можно использовать переменные, заключенные в фигурные кавычки. Задать значения переменных можно
в свойстве queryParams.

Пример объявления запроса к БД:
```groovy
// Регистрируем запрос ora:query1 к БД Oracle
query('ora:query1', true) {
  // Соединение к Oracle
  useConnection oracleConnection('ora:con1')
  
  // Текст запроса
  query = 'SELECT Count(DISTINCT field1) AS count_field1 FROM schema1.table1 WHERE field2 > {start}'
  
  // Параметры, используемые в запросе
  queryParams.start = 0
}

// Регистрируем запрос ver:query1 к БД Vertica
query('ver:query1', true) {
  // Соединение к Vertica
  useConnection verticaConnection('ver:con1')

  // Указать ресурсный файл проекта, в котором хранится скрипт запроса
  scriptFilePath = 'resource:/project/queries/ver.query1.sql' 
}
```

## <a name="fields"></a>Получение списка полей таблиц датасетов
Для существующих таблиц, представлений или при описании SQL запросов не требуется описывать их поля, список полей
будет автоматически получен при первом обращении к ним. 

При необходимости можно явно перечитать список полей у таблицы или представления БД с помощью метода retrieveFields:
```groovy
oracleTable('ora:table1') {
  // Получить список полей таблицы из БД
  retrieveFields()
}

view('mssql:view1') {
  // Получить список полей представления из БД
  retrieveFields()
}
```

Если для таблицы или представления будет указан явный набор полей, то при работе с таким датасетом он будет использоваться
при чтении и записи данных, метаданные с БД считываться не будут. При этом те поля таблицы или представления, которые 
не были указаны в списке полей датасета будут игнорироваться при работе с источником. 

P.S. При указании неполного списка полей для таблицы, которые имеют NOT NULL и не имеют DEFAULT в описании таблицы, 
при попытке записи в такую таблицу возникнет ошибка.

## <a name="deleting"></a>Удаление записей в таблицах БД
Для удаление записей в таблицах БД по условиям и полной очистка таблицы используйте методы deleteRows и truncate:
```groovy
mssqlTable('mssql:table1') {
  // Удалить записи по заданному условию
  deleteRows 'field2 < 1'
  
  // Очистить полностью таблицу
  truncate()
}
```

## <a name="queries"></a>Выполнение запросов к таблицам БД
Для получения количества записей таблицы или представления используйте метод countRow:
```groovy
assert view('mssql:view1').countRow('field1 IS NOT NULL') > 0
```

Для выполнения запроса над таблицей или представлением используйте метод select, в SQL запросе которого вместо явного 
имени таблицы можно использовать макропеременную {table}:
```groovy
verticaTable('ver:table1') {
  // Получить список уникальных field1
  def listField1 = select('SELECT DISTINCT field1 FROM {table} ORDER BY 1')
  println '*** Field1 list:'
  listField1.each { row ->
    println row.field1
  }
}
```

## <a name="reads"></a>Чтение данных датасетов
При чтении записей можно задать лимит количества считанных записей и какое количество записей следует пропустить:
```groovy
view('mssql:view1') {
    // Опции чтения
    readOpts {
      // Считывать не более 100 записей
      limit = 100
      // Начинать считывание, пропустив 200 записей
      offs = 200 
      // Сортировка при чтении
      order = ['field1', 'field2']
    }
    // Обработать записи источника
    echRow { row ->
        println "field1: ${row.field1}, field1: ${row.field2}, field1: ${row.field3}, " +
                "field4: ${row.field4}, field5: ${row.field5}"
    }
    assert readRows <= 100
}
```
В случае указания лимита и смещения не забывайте задавать явную сортировку для чтения датасета, иначе результат будет
недостоверным.

Для таблиц БД при чтении записей можно задать условия фильтрации и сортировки и установить дополнительные опции от вендора:
```groovy
verticaTable('ver:table1') {
    // Опции чтения
    readOpts { 
      // Фильтрация по заданному условию
      where = 'field2 > {start}'
      // Параметры для формирования запроса к таблице
      queryParams.start = 0
      // Сортировка по заданным полям
      order = ['field1', 'field2']
      
      // Профилировать выполнение запроса с именем profile1 в таблицу профилирования Vertica
      label = 'profile1'
      
      // Вернуть 25% сэмплированных данных от общего набора
      tablesample = 25
    }
    // Обработать записи источника
    echRow { row ->
        println "field1: ${row.field1}, field1: ${row.field2}, field1: ${row.field3}, " +
                "field4: ${row.field4}, field5: ${row.field5}"
    }
}
```

## <a name="writes"></a>Запись данных в таблицу БД
Запись в датасеты JDBC производится с помощью подготовленных пакетных операторов. Для оптимизации скорости записи можно 
изменить размер пакета записей, который уходит на сервер в опциях записи датасета:
```groovy
oracleTable('ora:table1') {
  // Опции записи в таблицу
  writeOpts {
    // Посылать на сервер пакеты по 100 записей (по умолчанию 500)
    batchSize = 100
  }
}

// Запись в таблицу Oracle
rowsTo(oracleTable('ora:table1')) {
  // Код записи в источник
  writeRow { adder ->
    adder field1: 'a', field2: 1, field3: new Date(), field4: new Date(), field5: 123.45
    adder field1: 'b', field2: 2, field3: new Date(), field4: new Date(), field5: 234.56
  }
}
```

## <a name="copy"></a>Копирование записей в другой источник
Для копирования используйте стандартный оператор etl.copyRows:
```groovy
// Копирование записей из таблицы Oracle в таблицу Vertica
etl.copyRows(oracleTable('ora:table1'), verticaTable('ver:table1'))

// Проверка количества записей
assert oracleTable('ora:table1').readRows == verticaTable('ver:table1').countRow() 
```

## <a name="bulkload"></a>Пакетная загрузка CSV файлов в таблицу
Для РСУБД H2, Impala, Hive и Vertica поддерживается пакетная загрузка CSV файлов:
```groovy
// Зарегистрировать временный CSV файл на основе структуры таблицы Vertica  
csvTempWithDataset('csv:forbulk', verticaTable('ver:table1'))

// Выгрузить в файл записи из Oracle таблицы
etl.copyRows(oracleTable('ora:table1'), csvTemp('csv:forbulk'))

// Работа с таблицей Vertica
verticaTable('ver:table1') {
  // Загрузить указанный файл в таблицу Vertica
  bulkLoadCsv(csvTemp('csv:forbulk')) {
    // При обнаружении ошибок записывать в указанный файл, путь для которого указан в переменной ОС WORK_PATH
    exceptionPath = '{WORK_PATH}/vertica.bulkload.err'
    // При обнаружении некорректных значений полей записей записывать их в указанный файл, 
    // путь для которого указан в переменной ОС WORK_PATH
    rejectedPath = '{WORK_PATH}/vertica.bulkload.err'

    // Удалять файлы после успешной загрузки
    removeFile = true
  }
}
```

Если требуется загрузить множество файлов по маске, используйте пакетный режим загрузки по маске:
```groovy
// Включить разделение файлов при записи по 100 мб
csvTemp('csv:forbulk') {
  // Имя файла
  fileName = 'data'
  // Расширение файла
  extension = 'csv'
  // Опции записи
  writeOpts {
    // Разбивать по 100 мб при записи
    splitSize = 100 * 1024 * 1024
  }
}

// Выгрузить в файлы записи из Oracle таблицы
etl.copyRows(oracleTable('ora:table1'), csvTemp('csv:forbulk'))

// Работа с таблицей Vertica
verticaTable('ver:table1') {
  // Загрузить указанный файл в таблицу Vertica
  bulkLoadCsv(csvTemp('csv:forbulk')) {
    // Задать простую маску имени загружаемых файлов
    files = 'data.*.csv'
    // Или задать список имен загружаемых файлов
    files = ['data.0001.csv', 'data.0002.csv', 'data.0003.csv']
    // Или задать сложную маску имени загружаемых файлов
    files = filePath {
      // Маска поиска
      mask = 'data.{num}.csv'
      // Определить переменные маски
      variable('num') { type = integerFieldType; length = 4 }
    }

    // Загружать все найденные файлы группой в одном операторе загрузки в таблицу (работает только для Vertica таблиц)
    // В обратном случае каждый файл будет загружаться отдельной командой загрузки в таблицу
    loadAsPackage = true
    
    // При обнаружении ошибок записывать в указанный файл, путь для которого указан в переменной ОС WORK_PATH
    exceptionPath = '{WORK_PATH}/vertica.bulkload.err'
    // При обнаружении некорректных значений полей записей записывать их в указанный файл, 
    // путь для которого указан в переменной ОС WORK_PATH
    rejectedPath = '{WORK_PATH}/vertica.bulkload.err'

    // Удалять файлы после успешной загрузки
    removeFile = true
  }
}
```

В команде copyRows так же поддерживается опция перезагрузки данных из источника в приемник с помощью промежуточного 
временного CSV файла:
```groovy
// Выгрузить в файлы записи из Oracle таблицы в таблицу Impala
etl.copyRows(oracleTable('ora:table1'), impalaTable('imp:table1')) {
  // Выгрузить данные из источника в временный CSV файл и загрузить его в приемник пакетной загрузкой
  bulkLoad = true
  // При выгрузке формировать упакованный в GZ CSV файл
  bulkAsGZIP = true
}
```

## <a name="copytable"></a>Копирование записей в другую таблицу источника
Для копирования записей из одной таблицы БД в другую таблицу используйте метод copyTo:
```groovy
// Создать таблицу приемник в БД
verticaTable('ver:table2', true) {
  schemaName = 'schema1'
  tableName = 'table2'

  // Поля датасета, при работе в Getl приводятся к нижнему регистру
  field('f1') { type = stringFieldType; length = 50 }
  field('f2') { type = integerFieldType }
  field('f3') { type = dateFieldType }
  
  // Создать, если не существует или очистить, если существует
  if (!exists) create() else truncate()
}

// Таблица источник
verticaTable('ver:table1') {
  // Задать опции чтения таблицы
  readOpts {
    // Фильтр выборки записей
    where = 'field1 BETWEEN 1 AND 1000'
    
    // Сэмплировать и вернуть 30% от результата выборки
    tablesample = 30
  }
  
  // Установить маппинг копируемых полей приемника к источнику
  def map = [f1: field1, f2: field2, f3: field3]
  
  // Скопировать записи в приемник
  copyTo verticaTable('ver:table2'), map
}
```
Для выполнения операции будет выполнен SQL скрипт INSERT SELECT. При формировании списка копируемых полей будут браться
одинаковые имена полей из таблиц приемника и источника. Если указан маппинг, то указанные имена полей приемника будут
ассоциированы с указанными именами полей источника. Если у таблицы источника указана фильтрация where или другие опции
чтения таблицы, они будут указаны в SELECT. Если указаны хинты и опции записи таблицы приемника, они будут указаны
в INSERT.

# <a name="sql"></a>Выполнение пакетных сценариев SQL
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

## <a name="sql_exec"></a>Выполнение текста SQL скрипта
Для выполнения ХП скриптов используется метод exec в операторе sql:
```groovy
// Выполнить SQL скрипт
sql {
  // Использовать указанное соединение Vertica
  useConnection verticaConnection('ver:con1')

  // Установить значение переменным для скрипта
  vars.param1 = 1
  vars.param2 = 'ok'
  vars.param3 = new Date()

  // Выполнить скрипт из текста
  exec '''
/*:count_row*/ 
UPDATE schema1.table1 
SET field4 = '{param3}'::timestamp 
WHERE field2 = {param1} AND field1 = '{param2}'
    '''
  // Проверить полученное значение переменной из скрипта
  assert vars.count_row == 1, 'Не удалось обновить запись!'
}
```

## <a name="sql_vars"></a>Работа с переменными в скриптах
При выполнении ХП Getl вместо {переменных} подставляет их значения. При использовании текстовых переменных внутри скриптов следует их
обрамлять в одинарные кавычки. Для переменных с типом дата-время Getl подставляет текстовое значение в формате "yyyy-MM-dd HH:mm:ss".
Если перед DML оператором поставить "_/*:переменная*/_", то количество обработанных записей будет занесено в эту переменную и она будет доступна
в vars.

## <a name="sql_file"></a>Выполнение SQL скрипта из файла
Скрипты можно выносить в отдельные файлы, в том числе ресурсные и вызывать их с помощью метода runFile в
операторе sql.

Пример файла "/sql/script.sql", который располагается в ресурсах проекта:
```genericsql
-- Вывести сообщение на консоль
ECHO Расчет региона {regionid} ...

-- Получение периода расчета в переменные по именам полей из SELECT
SET SELECT Min(hour) as hour_start, Max(hour) as hour_finish, Count(*) AS count_rows
    FROM schema1.fact1
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
    FROM schema1.fact1
    WHERE regionid = {regionid} AND hour BETWEEN '{hour_start}'::timestamp AND '{hour_finish}'::timestamp;

  UPDATE schema1.fact1
  SET status = 'complete'
  WHERE regionid = {regionid} AND hour = '{hour}'::timestamp

  ECHO Час {hour} успешно рассчитан.
END FOR; 
```
P.S. При обработке записей в цикле, оператор FOR сначала получит в память весь массив записей и потом
начнем по нему обработку. Не рекомендуется возвращать в SELECT большое количество записей.

Пример вызова ресурсного SQL файла:
```groovy
// Выполнить SQL скрипт
sql {
  // Использовать указанное соединение
  useConnection verticaConnection('ver:con1')

  // Установить значение переменным скрипта
  vars.regionid = 1

  // Выполнить скрипт из ресурсного файла
  runFile 'resource:/sql/script.sql'

  logInfo "Было обработано ${vars.count_rows} записей"
}
```  