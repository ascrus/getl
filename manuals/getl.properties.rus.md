# Содержание
* [Опции движка](#engine)
* [Управление конфигурациями](#config)
* [Управление логированием](#log)
* [Управление репозиторием](#repository)
* [Настройка профилирования](#profile)
* [Вынос опций Getl в ресурсный файл](#getlproperties)
* [Вынос опций проекта в ресурсный файл](#projproperties)
* [Указание опций в командной строке](#commandline)

# <a name="engine"></a>Опции движка

# <a name="config"></a>Управление конфигурациями

# <a name="log"></a>Управление логированием

# <a name="repository"></a>Управление репозиторием

# <a name="profile"></a>Настройка профилирования

# <a name="getlproperties"></a>Вынос опций Getl в ресурсный файл
Для того, чтобы задать опции Getl сразу при старте процессов проекта можно их вынести в ресурсный файл 
getl-properties.conf. Если этот файл обнаружен при запуске лаунчера Getl, то он используется как инициализация опций 
движка.

Структура файла конфигурации:
```groovy
// Опции проекта 
project {
    // Список требуемых для запуска переменных ОС 
    needEnvironments = ['PROJ_ENV1', 'PROJ_ENV2']
    
    // Имя ресурсного файла проекта, который требуется автоматически считать при старте лаунчера
    configFileName = 'resource:/project-properties.conf'
}

// Параметры репозитория
repository {
    // Путь к файлам репозитория
    path = 'resource:/repository'
    // Ключ шифрования паролей репозитория
    encryptKey = 'project-password-key'
    
    // Подгружать описания объектов из файлов репозитория при обращении к ним
    autoLoadFromStorage = true
    // Подгружать описания объектов из файлов репозитория при их поиске с помощью list и process
    autoLoadForList = true

    // Путь для сохранения файлов репозитория при вызове процессов RepositorySave
    repositorySavePath = 'src/main/resources/repository'
}

// Опции логирования
logging {
    // Путь для записи сообщений в лог файл
    logFileName = '{PROJ_ENV1}/logs/{process}/{date}.log'
    // Путь для логирования SQL команд, посылаемых в JDBC источники
    jdbcLogPath = '{PROJ_ENV1}/logs/jdbc'
    // Путь для логинования команд, посылаемых в файловые источники
    filesLogPath = '{PROJ_ENV1}/logs/files'
    // Путь для логирования SQL команд, посылаемых встроенной БД
    tempDBLogFileName = '{PROJ_ENV1}/logs/tempdb'
}

// Опции движка Getl
engine {
    // Запустить указанный класс инициализации до запуска лаунчером самого процесса
    initClass = 'my.project.init.InitClass'
    
    // Датасет контроля запуска процессов
    controlDataset = 'ver.monitor:s_processes'
    // При обращении к датасету использовать отдельное соединение под указанным логином
    controlLogin = 'service_user'
    // Проверять при старте лаунчером разрешение для запускаемого процесса в датасете контроля процессов
    controlStart = true
    // Проверять при старте каждого потока thread разрешение для выполняемого процесса в датасете контроля процессов  
    controlThreads = false
    
    // Включить клонирования объектов при обращении к ним из потоков thread
    useThreadModelCloning = true
}
// Опции профилирования
profile {
    // Разрешить профилированрие выполняемых операторов Getl
    enabled = true
    // Уровень вывода сообщений профилирования в логе
    level = java.util.logging.Level.FINER
    // Уровень вывода команды ECHO из SQL скриптов в логе
    sqlEchoLevel = java.util.logging.Level.FINE
    // Включить режим отладки для более детальной информации при профилировании
    debug = false
}
```

# <a name="getlproperties"></a>Вынос опций проекта в ресурсный файл
Общие опции проекта, которые используются в процессах, можно вынести в собственный файл конфигурации и указать 
в getl-properties его для загрузки в project.configFileName. Если в конфигурационном файле проекта указаны версия, года
разработки, название проекта и компании разработчика, то при старте лаунчером процесса это будет выведено в лог.

Переменные конфигурационного файла проекта будут загружены в options.projectConfigParams и доступны для использования 
в коде.

Структура файла конфигурации:
```groovy
// Версия проекта
version='1.00'
// Года разработки
year='2010-2020'
// Наименование проекта
project='My project'
// Название компании разработчика проекта
company='My company'

// Другие параметры
regions=['Moscow', 'Sankt Peterburg', 'Kazan']
```

Пример использования переменной regions в коде процесса:
```groovy
println "Регионы конфигурации: ${options.projectConfigParams.regions}"
```

Если вы хотите автоматически собирать ресурсный файл конфигурации проекта, включая в него версию компиляции проекта
gradle, добавьте в build.gradle задачу генерации ресурсного файла: 
```groovy
processResources.dependsOn "generateProjectConfig"

task("generateProjectConfig") {
    def resDir = "$buildDir/resources/main"
    doLast {
        file(resDir).mkdirs()
        def curYear = LocalDateTime.now().year
        file("$resDir/${project.name}.conf").text = """/* 
Application Version Configuration File
Generated: ${LocalDateTime.now().format('yyyy-MM-dd HH:mm')} 
*/
version='$version'
year='2010-$curYear'
project='My project'
company='My company'
regions=['Moscow', 'Sankt Peterburg', 'Kazan']
"""
    }
}
```

# <a name="commandline"></a>Указание опций в командной строке
