package getl.examples.hive

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Generate sample data in a H2  database
runGroovyClass getl.examples.h2.Install

// Load configuration file
runGroovyClass getl.examples.hive.Config
// Define object as Hive tables
runGroovyClass getl.examples.hive.Tables

profile("Create Hive objects") {
    // Run sql script for create schemata and tables
    sql {
        exec 'CREATE SCHEMA IF NOT EXISTS getl_demo;'
        logInfo'Created schema getl_demo.'
    }

    processDatasets(HIVETABLE) { tableName ->
        hiveTable(tableName) { table ->
            if (!table.exists) {
                // Create table in database
                create ifNotExists: true
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
    run(listDatasets(HIVETABLE)) { tableName ->
        // Copy rows from the embedded table to the Hive table
        copyRows(embeddedTable(tableName), hiveTable(tableName)) {
            bulkLoad = true
            done { logInfo "Copied $countRow rows of $tableName from the embedded table to the Hive table" }
        }
    }
}

thread {
    addThread {
        assert hiveTable('prices').countRow() == 7
    }
    addThread {
        assert hiveTable('customers').countRow() == 3
    }
    addThread {
        assert hiveTable('customers.phones').countRow() == 7
    }
    addThread {
        assert hiveTable('sales').countRow() == 250000
    }

    exec()
}
