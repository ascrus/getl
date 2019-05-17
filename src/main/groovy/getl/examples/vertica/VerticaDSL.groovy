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
    connectDatabase = '<vertica database name>'
    connectHost = '<vertica node host>'
    login = '<user name>'
    password = '<user password>'
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
        connectDatabase = configVars.connectDatabase
        connectHost = configVars.connectHost
        login = configVars.login
        password = configVars.password
        sqlHistoryFile = "${configVars.workPath}/vertica.sql"
    }

    // Table name
    tableName = 'table1'

    // Drop table if exists
    if (exists) logWarn "Detected exist table ${fullNameDataset()}"
    dropTable { ifExists = true }

    // Define fields
    addField field { name = 'id'; type = integerFieldType; isKey = true }
    addField field { name = 'name'; type = stringFieldType; length = 50; isNull = false }
    addField field { name = 'dt'; type = datetimeFieldType; isNull = false }
    addField field { name = 'value'; type = numericFieldType; length = 12; precision = 2 }

    // Create table
    createTable { ifNotExists = true }
    logInfo "Created table ${fullNameDataset()}"
}

// Define CSV file
def file1 = csv { CSVDataset file ->
    // Define CSV connection
    connection = csvConnection {
        // Path of files
        path = configVars.workPath
        // Field delimiter
        fieldDelimiter = '|'

        // Code page file
        codePage = 'UTF8'

        // Use GZIP copression
        isGzFile = true
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

    // Use COPY method Vertica for load rows
    bulkLoad = true

    // Transformation code source row to destination row
    process { file, table ->
        table.name = file.name?.toUpperCase()
    }
}
logInfo "Copyed ${table1.updateRows} from \"${file1.fullFileName()}\" to ${table1.fullNameDataset()}."

// Read rows from table1
rowProcess {
    // Source dataset
    source = verticatable(table1) { order = ['id']; queryDirective.label = 'Read table1' }

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
assert table1.updateRows == table1_rows
logInfo "Readed ${table1.readRows} rows from ${table1.fullNameDataset()} table."