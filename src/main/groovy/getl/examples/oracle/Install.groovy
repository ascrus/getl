package getl.examples.oracle

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Generate sample data in a H2  database
runGroovyClass getl.examples.h2.Install

// Load configuration file
runGroovyClass getl.examples.oracle.Config
// Define object as Oracle tables
runGroovyClass getl.examples.oracle.Tables

profile("Create Oracle objects") {
    processDatasets(ORACLETABLE) { tableName ->
        oracleTable(tableName) { table ->
            if (!table.exists) {
                // Create table in database
                create()
                logInfo "Created table $tableName."
            }
            else {
                truncate()
                logInfo "Truncated table $tableName."
            }
        }
    }
}

thread {
    run(listDatasets(ORACLETABLE)) { tableName ->
        // Copy rows from the embedded table to the Oracle table
        copyRows(embeddedTable(tableName), oracleTable(tableName)) {
            done { logInfo "Copied $countRow rows of $tableName from the embedded table to the Oracle table" }
        }
    }
}

thread {
    addThread {
        assert oracleTable('prices').countRow() == 7
    }
    addThread {
        assert oracleTable('customers').countRow() == 3
    }
    addThread {
        assert oracleTable('customers.phones').countRow() == 7
    }
    addThread {
        assert oracleTable('sales').countRow() == 250000
    }

    exec()
}
