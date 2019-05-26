@BaseScript getl.lang.Getl getl

import getl.csv.CSVDataset
import getl.utils.GenerationUtils
import getl.vertica.VerticaTable
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
    workPath = '<log and history files directory>'
    driverPath = '<vertica jdbc file path>'
    connectDatabase = '<vertica database name>'
    connectHost = '<vertica node host>'
    login = '<vertica user name>'
    password = '<vertica user password>'
    ssh_login = '<ssh login on Vertica host>
    ssh_password = '<ssh password on Vertica host>
    ssh_rsakey = '<ssh rsa string key for host>' // use "ssh-keyscan -t rsa <host-name>"
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
def table1 = verticatable { VerticaTable table ->
    // Define Vertica connection with using configuration parameters
    connection = verticaConnection {
        driverPath = configVars.driverPath
        connectHost = configVars.connectHost
        connectDatabase = configVars.connectDatabase
        login = configVars.login
        password = configVars.password
        sqlHistoryFile = "${configVars.workPath}/vertica.sql"

        configVars.session = currentSession
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
        isGzFile = true
    }

    // File name
    fileName = 'file1.csv.gz'

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
        assert table1.readRows == table1_rows
        logInfo "Readed ${table1.readRows} rows from ${table1.fullTableName} table."
    }
}

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

sftp {
    server = configVars.connectHost
    rootPath = '/tmp'
    localDirectory = csvTempConnection.path
    login = configVars.ssh_login
    password = configVars.ssh_password
    hostKey = configVars.ssh_rsakey
    connect()
    if (!existsDirectory('vertica')) createDir 'vertica'
    changeDirectory 'vertica'
    upload file1.fileName
    logInfo "Copyed file \"${file1.fullFileName()}\" to \"${currentDir()}\" directory from \"${server}\" host."
}

verticatable(table1) {
    truncate()
    bulkLoadOpts {
        def dir = '/tmp/vertica'
        source = file1
        files = ["$dir/${file1.fileName}"]

        parser {
            function = 'fcsvparser'
            options = [type: 'traditional', delimiter: '|', record_terminator: '\n']
            useCsvOptions = false
        }
        autoCommit = true
        loadMethod = DIRECT
        enforceLength = true
        compressed = GZIP
        exceptionPath = "$dir/vertica.exeptions.txt"
        rejectedPath = "$dir/vertica.reject.txt"
        rejectMax = 100
        location = configVars.session.node_name
        streamName = 'GETL LOAD'

    }
    bulkLoadFile()
}

assert table1.countRows() == table1_rows