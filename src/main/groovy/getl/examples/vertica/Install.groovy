package getl.examples.vertica

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Generate sample data in a H2  database
runGroovyClass getl.examples.h2.Install

// Load configuration file
runGroovyClass getl.examples.vertica.Config
// Define object as Vertica tables
runGroovyClass getl.examples.vertica.Tables

profile("Create Vertica objects") {
    // Run sql script for create schemata and tables
    sql {
        exec 'CREATE SCHEMA IF NOT EXISTS getl_demo;'
        logInfo'Created schema getl_demo.'
    }

    processDatasets(VERTICATABLE) { tableName ->
        verticaTable(tableName) { table ->
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

thread(listDatasets(VERTICATABLE)) {
    run(2) { tableName ->
        // Copy rows from the embedded table to the Vertica table
        copyRows(embeddedTable(tableName), verticaTable(tableName)) { source, dest ->
            bulkLoad = true
            done { logInfo "Copied $countRow rows of $tableName from the embedded table to the Vertica table" }
        }
    }
}

thread {
    addThread {
        assert verticaTable('prices').countRow() == 7
    }
    addThread {
        assert verticaTable('customers').countRow() == 3
    }
    addThread {
        assert verticaTable('customers.phones').countRow() == 7
    }
    addThread {
        assert verticaTable('sales').countRow() == 250000
    }

    exec()
}
