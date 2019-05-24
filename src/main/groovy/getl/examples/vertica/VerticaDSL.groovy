@BaseScript getl.lang.Getl getl

import getl.csv.CSVDataset
import getl.utils.GenerationUtils
import groovy.transform.BaseScript

def table1_rows = 10000

// Enabled process timing
options {
    processTimeTracing = true
}

/*
Configuration options

Create config file in ./tests/vertica/vertica.dsl with syntax:
vars {
    workPath = '<work directory>'
    vertica {
        connectDatabase = '<vertica database name>'
        connectHost = '<vertica node host>'
        login = '<vertica user name>'
        password = '<vertica user password>'
    }
    ftp {
        server = '<ftp server host>'
        rootPath = '<ftp root directory>'
        login = '<ftp user name>'
        password = '<ftp user password>'
    }
}
*/
config {
    // Directory of configuration file
    path = 'tests/vertica'

    // Load configuration file
    load'vertica.dsl'

    logConfig "Load configuration vertica.dsl complete. Use directory \"${configVars.workPath}\"."
}

// Logger options
log {
    // Log file name with {date} variable in name
    logFileName = "${configVars.workPath}/vertica.{date}.log"
}

// Define Vertica table
def table1 = verticatable {
    // Define Vertica connection with using configuration parameters
    connection = verticaConnection {
        driverPath = '../jdbc/vertica-jdbc-9.1.1-0.jar'
        connectDatabase = configVars.vertica.connectDatabase
        connectHost = configVars.vertica.connectHost
        login = configVars.vertica.login
        password = configVars.vertica.password
        sqlHistoryFile = "${configVars.workPath}/vertica.sql"
    }

    // Table name
    tableName = 'table1'

    // Define fields
    addField field { name = 'id'; type = integerFieldType; isKey = true }
    addField field { name = 'name'; type = stringFieldType; length = 50; isNull = false }
    addField field { name = 'dt'; type = datetimeFieldType; isNull = false }
    addField field { name = 'value'; type = numericFieldType; length = 12; precision = 2 }

    // Create options
    createOpts{
        ifNotExists = true
        orderBy = ['dt', 'id']
        segmentedBy = 'Hash(id) ALL NODES KSAFE 1'
        partitionBy = 'Year(dt)'
    }

    // Drop options
    dropOpts {
        ifExists = true
    }

    // Read options
    readOpts {
        label = 'Read query'
        where = "id between 1 and $table1_rows"
        order = ['id']
    }

    writeOpts {
        label = 'Write query'
        direct = DIRECT
        batchSize = 10000
    }

    // Drop table if exists
    if (exists) {
        logWarn "Detected exist table $fullTableName}"
        drop()
        logInfo "Dropped table $fullTableName"
    }

    // Create table
    create()

    logInfo "Created table $fullTableName"
}

// Define CSV file
def file1 = csv { CSVDataset file ->
    // Define CSV connection
    connection = csvTempConnection {
        // Field delimiter
        fieldDelimiter = '|'

        // Code page file
        codePage = 'UTF8'

        // Use GZIP copression
        isGzFile = false
    }

    // File name
    fileName = 'file1.csv'

    // Define fields as table fields
    field = table1.field

    // Write rows to file
    rowsTo {
        dest = file

        // Write code rows to destination
        process { save -> // writer object
            (1..table1_rows).each {
                save id: it, name: "row $it", dt: now, value: GenerationUtils.GenerateNumeric(12, 2)
            }
        }
    }
    logInfo "Writed ${file.writeRows} rows to \"${file.fullFileName()}\" csv file."
}

// Copy rows from file1 to table1
copyRows {
    // Source dataset
    source = file1

    // Destination dataset
    dest = table1

    // Transformation code source row to destination row
    process { file, table ->
        table.name = file.name?.toUpperCase()
    }
}
logInfo "Copyed ${table1.updateRows} from \"${file1.fullFileName()}\" to ${table1.fullTableName}."

// Read rows from table1
rowProcess {
    // Source dataset
    source = table1

    def i = 0

    // Process code for readed row
    process {
        i++
        assert i == it.id
    }
    done {
        assert countRow == i
    }
}
assert table1.readRows == table1_rows
logInfo "Readed ${table1.readRows} rows from ${table1.fullTableName} table."

sql {
    connection = table1.connection
    script { """
ECHO Run sql script on [{vertica.connectDatabase}] database ...
SET SELECT CURRENT_TIMESTAMP as tran_time, Count(*) AS table1_rows FROM ${table1.fullTableName};
ECHO Found {table1_rows} rows in ${table1.fullTableName} on {tran_time} transaction time.
    """ }
    done {
        logInfo "Sql count detect $countRow rows in ${table1.fullTableName} table."
        assert vars.table1_rows != null
    }
}

ftp {
    server = configVars.ftp.server
    rootPath = configVars.ftp.rootPath
    localDirectory = csvTempConnection.path
    login = configVars.ftp.login
    password = configVars.ftp.password
    connect()
    if (!existsDirectory('archive')) createDir 'archive'
    changeDirectory 'archive'
    upload file1.fileName
    logInfo "Copyed file \"${file1.fullFileName()}\" to \"${currentDir()}\" directory from \"${server}\" host."
}