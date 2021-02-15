# Содержание
* [Расширенная поддержка Micro Focus Vertica](#extended)
* [Работа с партициями таблиц](#partitions)
* [Копирование данных между кластерами Vertica](#copycluster)
* [Дефрагментация таблиц Vertica](#defrag)
* [Обработка рекомендаций workload анализатора запросов Vertica](#workload)

# <a name="extended"></a>Расширенная поддержка Micro Focus Vertica
Getl имеет расширенную поддержку работы с аналитическим хранилищем данных Micro Focus Vertica для своих объектов:
* verticaConnection: подключение и отключение внешних кластеров Vertica для копирования данных таблиц между ними, пересчет статистики таблиц,
  дефрагментация таблиц, запуск встроенной функции Vertica для анализа работы запросов и пересчет статистики по полученным рекомендациям.
* verticaTable: копирование и очистка партиций у таблиц, создание таблиц по шаблону с других таблиц, пересчет статистики и дефрагментация таблицы.

# <a name="partitions"></a>Работа с партициями таблиц
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
useVerticaConnection verticaConnetion('ver:con1', true) {
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

# <a name="copycluster"></a>Копирование данных между кластерами Vertica
Для копирования записей между кластерами используйте методы attachExternalVertica и detachExternalVertica для подключения
и отключения внешнего кластера Vertica к текущей сессии:
```groovy
// Соединение к приёмнику Vertica
verticaConnection('ver:con2', true) {
  connectHost = 'vertica-ext-host1'
  connectDatabase = 'db1'
  login = 'user'
  password = 'password'
}

verticaConnection('ver:con1') {
  // Добавление внешнего соединения источника данных к текущему соединению
  attachExternalVertica verticaConnection('ver:con2')
  // Выполнение команды копирования между кластерами
  executeCommand 'COPY public.table1 FROM VERTICA db2.public.table1'
  // Отсоединение источника от текущего соединения
  detachExternalVertica verticaConnection('ver:con2')
}
``` 
P.S. Getl запоминает, какие внешние подключения были связаны с соединением Vertica. Не удастся второй раз присоединить тот же
кластер Vertica или разъединить не присоединенный ранее кластер.

# <a name="defrag"></a>Дефрагментация таблиц Vertica
Для дефрагментации таблиц используйте метод purgeTables, для пересчета статистики таблиц analyzeStatistics:
```groovy
// Соединение к Vertica
verticaConnection {
  connectHost = 'vertica-host1'
  connectDatabase = 'verdb'
  login = 'user'
  password = 'password'

  // Дефрагментировать все таблицы, у которых более 15% удаленных записей от общего количества
  purgeTables 15
  // Пересчитать статистику всех таблиц БД
  analyzeStatistics()
}
``` 

# <a name="workload"></a>Обработка рекомендаций workload анализатора запросов Vertica
Для получения списка рекомендаций workload используйте метод analyzeWorkload, для его обработки метод processWorkload:
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