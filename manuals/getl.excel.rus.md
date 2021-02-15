# Содержание
* [Объекты языка](#objects)
* [Объявление Excel источника](#declare)
* [Чтение данных с Excel файла](#reads)
* [Копирование записей в другой источник](#copy)
* [Многопоточная загрузка Excel файлов](#processing)

# <a name="objects"></a>Объекты языка
В Getl поддерживается два объекта работы с Excel:
* excelConnection: соединение, которое указывает, где лежат файлы Excel
* excel: датасет, который читает данные из листа Excel

# <a name="declare"></a>Объявление Excel источника
Требуется в соединении задать путь хранения файлов Excel и в датасет описать параметры данных листа Excel и читаемых с него полей:
```groovy
// Регистрируем в репозитории соединение excel:con1
excelConnection('excel:con1', true) {
    // Путь хранения файлов (должна быть указанна переменная ОС EXCEL_PATH)
    path = '{EXCEL_PATH}/files'
}

// Регистрируем в репозитории датасет excel:data1
excel('excel:data1', true) {
    useConnection excelConnection('excel:con1')
    // Имя файла
    fileName = 'excel_2020-12-31.xlsx'
    // Имя листа
    listName = 'List1'
    // Первая строка заголовок
    header = true
    
    // Поля листа, перечисляются по порядку следования колонок листа
    field('field1') { type = stringFieldType; length = 50 }
    field('field2') { type = integerFieldType }
    field('field3') { type = dateFieldType }
    field('field4') { type = datetimeFieldType }
    field('field5') { type = numericFieldType; length = 12; precision = 2 }
}
```

# <a name="reads"></a>Чтение данных с Excel файла
При чтении записей можно задать лимит количества считанных записей:
```groovy
excel('excel:data1') {
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

# <a name="copy"></a>Копирование записей в другой источник
Для копирования используйте стандартный оператор etl.copyRows:
```groovy
// Создать таблицу в БД
embeddedTable('dwh:buf', true) {
    tableName = 'excel_data'
    field = excel('excel:data1').field
    create()
}

// Выключить лимит
excel('excel:data1') {
    readOpts { limit = null }
}

// Копирование записей из Excel файла в таблицу БД
etl.copyRows(excel('excel:data1'), embeddedTable('dwh:buf'))

// Проверка количества записей
assert excel('excel:data1').readRows == embeddedTable('dwh:buf').countRow() 
```

# <a name="processing"></a>Многопоточная загрузка Excel файлов
Для параллельной загрузки множества файлов используйте стандартный оператор процессинга файлов
fileman.processing:
```groovy
// Таблица хранения истории загруженных файлов
embeddedTable('dwh:history', true) {
    tableName = 'history_excel_files'
}

// Регистрируем систему хранения исходных Excel файлов
files('excel:files', true) {
    // Путь поиска файлов для процессинга
    rootPath = '{EXCEL_PATH}/files'
    // Хранить историю загрузки файлов в указанной таблице
    story = embeddedTable('dwh:history')  
    // Создать таблицу при первом обращении
    createStory = true
}

// Процессинг файлов
fileman.processing(files('excel:files')) {
    // Маска поиска файлов
    useSourcePath {
        // Пример excel_2020-12-31.xlsx
        mask = 'excel_{date}.xlsx'
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

        excel('excel:data1') {
            // Установить имя обрабатываемого файла для датасета
            fileName = inf.attr.filename
        }

        // Копирование записей из Excel файла в таблицу БД
        etl.copyRows(excel('excel:data1'), embeddedTable('dwh:buf'))
        
        logInfo "Успешно загружено ${embeddedTable('dwh:buf').writeRows} записей из файла ${inf.file} в таблицу ${embeddedTable('dwh:buf')}"
        
        // Выставить успешный результат для процессинга
        inf.result = inf.completeResult
    }
}
```