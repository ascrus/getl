# Содержание
* [Объекты языка](#objects)
* [Временные файлы CSV](#temp)
* [Объявление CSV источника](#declare)
* [Чтение данных с CSV файла](#reads)
* [Запись данных в CSV файл](#writes)
* [Чтение порционных файлов](#readsplit)
* [Форматирование и парсинг полей](#formatting)
* [Копирование записей в другой источник](#copy)
* [Многопоточная загрузка CSV файлов](#processing)

# <a name="objects"></a>Объекты языка
В Getl поддерживается четыре объекта для работы с CSV:
* csvConnection: соединение, которое указывает, где лежат файлы CSV
* csv: датасет, который читает и пишет данные в файл CSV
* csvTempConnection: соединение, которое указывает, где лежат временные файлы CSV
* csvTemp: датасет, которые читает и пишет данные в временный файл CSV

# <a name="temp"></a>Временные файлы CSV
Для хранения промежуточных данных можно использовать временные файлы CSV, которые создаются во временном директории пользователя
и автоматически удаляются при завершении работы приложения. Для описания датасетов временных файлов не обязательно указывать соединение,
по умолчанию будет использовано общее соединение Getl для хранения таких файлов. При необходимости можно зарегистрировать собственное
соединение, задав для него явно собственный путь хранения временных файлов.

# <a name="declare"></a>Объявление CSV источника
Требуется в соединении задать путь хранения файлов CSV и в датасет описать параметры датасета CSV:
```groovy
// Регистрируем в репозитории соединение csv:con1
csvConnection('csv:con1', true) {
    // Путь хранения файлов (должна быть указанна переменная ОС CSV_PATH)
    path = '{CSV_PATH}/files'
    // Расширение файлов
    extension = 'csv'
    // Создать путь, если его нет
    createPath = true
}

// Регистрируем в репозитории датасет csv:data1
csv('csv:data1', true) {
    useConnection csvConnection('csv:con1')
    // Имя файла
    fileName = 'data_2020-12-31'
    // Первая строка заголовок
    header = true
    // Разделитель полей
    fieldDelimiter = ','
    // Интерпретировать кавычки как двойные в формате MS
    escaped = false
    // Интерпретировать константу <NULL> как NULL значение для поля
    nullAsValue = '<NULL>'
    // Кодировка файла
    codePage = 'cp1251'
    
    // Поля датасета, перечисляются по порядку следования колонок файла
    field('field1') { type = stringFieldType; length = 50 }
    field('field2') { type = integerFieldType }
    field('field3') { type = dateFieldType }
    field('field4') { type = datetimeFieldType }
    field('field5') { type = numericFieldType; length = 12; precision = 2 }
}
```

# <a name="reads"></a>Чтение данных с CSV файла
При чтении записей можно задать лимит количества считанных записей:
```groovy
csv('csv:data1') {
    // Опции чтения
    readOpts { limit = 100 }
    // Обработать записи источника
    echRow { row ->
        println "field1: ${row.field1}, field1: ${row.field2}, field1: ${row.field3}, " +
                "field4: ${row.field4}, field5: ${row.field5}"
    }
    assert readRows <= 100
}
```

# <a name="writes"></a>Запись данных в CSV файл
При записи данных можно указать размер разбивки файлов, это позволит писать длинные файлы порциями:
```groovy
csv('csv:data1') {
    // Опции записи
    writeOpts {
        // Разбивать при записи на файлы размером не более 100 мб
        splitSize = 100 * 1024 * 1024 
    }
}

// Запись в источник
rowsTo(csv('csv:data1')) {
    // Код записи в источник
    writeRow { adder ->
        adder field1: 'a', field2: 1, field3: new Date(), field4: new Date(), field5: 123.45
        adder field1: 'b', field2: 2, field3: new Date(), field4: new Date(), field5: 234.56
    }
}
```
Если запись идет в порционные файлы, то их имена будут собираться по маске "имя_csv_файла.<номер порции>.расширение",
где имя порции будет 4 значным числом.

# <a name="readsplit"></a>Чтение порционных файлов
Для чтения записей всех порций файлов установите в опции чтения флаг isSplit:
```groovy
csv('csv:data1') {
    // Опции чтения
    readOpts { isSplit = true; limit = null }
    // Обработать записи источника
    echRow { row ->
        println "field1: ${row.field1}, field1: ${row.field2}, field1: ${row.field3}, " +
                "field4: ${row.field4}, field5: ${row.field5}"
    }
}
```
При установленном флаге isSplit при чтении будут найдены и последовательно считаны все файлы по маске 
"имя_csv_файла.<номер порции>.расширение".

# <a name="formatting"></a>Форматирование и парсинг полей
При чтении и записи CSV файлов нестандартных форматов можно задать явные форматы для файла или для отдельных полей:
```groovy
csv('csv:data1') {
    // Читать и писать поля с датой в указанном формате
    formatDate = 'yyyy_MM_dd'
    // Читать и писать поля с таймстампом в указанном формате
    formatDateTime = 'yyyy_MM_dd HH-mm-ss'
    // Читать и писать не целочисленные числовые поля с указанным разделителем
    decimalSeparator = ','
    
    // Задать явный формат парсинга даты для поля field3
    field('field3') { format = 'yyyy.MM.dd' }
}
```
Вместо явного указания форматов для CSV файла можно задать локаль, правила которой следует использовать при парсинге
полей:
```groovy
csv('csv:data1') {
    locale = 'ru_RU'
}
```

# <a name="copy"></a>Копирование записей в другой источник
Для копирования используйте стандартный оператор etl.copyRows:
```groovy
// Создать таблицу в БД
embeddedTable('dwh:buf', true) {
    tableName = 'csv_data'
    field = csv('csv:data1').field
    create()
}

// Копирование записей из CSV файла в таблицу БД
etl.copyRows(csv('csv:data1'), embeddedTable('dwh:buf'))

// Проверка количества записей
assert csv('csv:data1').readRows == embeddedTable('dwh:buf').countRow() 
```

# <a name="processing"></a>Многопоточная загрузка CSV файлов
Для параллельной загрузки множества файлов используйте стандартный оператор процессинга файлов
fileman.processing:
```groovy
// Таблица хранения истории загруженных файлов
embeddedTable('dwh:history', true) {
    tableName = 'history_csv_files'
}

// Регистрируем систему хранения исходных CSV файлов
files('csv:files', true) {
    // Путь поиска файлов для процессинга
    rootPath = '{CSV_PATH}/files'
    // Хранить историю загрузки файлов в указанной таблице
    story = embeddedTable('dwh:history')  
    // Создать таблицу при первом обращении
    createStory = true
}

// Не использовать расширение для имен файлов при процессинге
csvConnection('csv:con1') { extension = null }

// Процессинг файлов
fileman.processing(files('csv:files')) {
    // Маска поиска файлов
    useSourcePath {
        // Пример data_2020-12-31.csv
        mask = 'data_{date}.csv'
        // Формат переменной из имени date
        variable('date') { type = dateFieldType; format = 'yyyy-MM-dd' }
    }

    // Удалять файлы после успешной обработки
    removeFiles = true
    // Загружать по возрастанию макропеременной date из имени файла
    order = ['date']
    // Загружать одновременно 3 файла
    countOfThreadProcessing = 3

    // Логика процессинга одного файла в потоке
    processFile { inf ->
        def date = inf.attr.date as Date
        logFinest "Загрузка файла ${inf.file} за $date ..."

        csv('csv:data1') {
            // Установить имя обрабатываемого файла для датасета
            fileName = inf.attr.filename
        }

        // Копирование записей из CSV файла в таблицу БД
        etl.copyRows(csv('csv:data1'), embeddedTable('dwh:buf'))
        
        logInfo "Успешно загружено ${embeddedTable('dwh:buf').writeRows} записей из файла ${inf.file} в таблицу ${embeddedTable('dwh:buf')}"
        
        // Выставить успешный результат для процессинга
        inf.result = inf.completeResult
    }
}
```