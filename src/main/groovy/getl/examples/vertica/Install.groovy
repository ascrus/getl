package getl.examples.vertica

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

/**
 * Create Vertica tables and load data from H2 tables to Vertica
 */

// Generate sample data in a H2  database
runGroovyClass getl.examples.h2.Install

// Define Vertica tables
runGroovyClass getl.examples.vertica.Tables, true

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

thread {
    run(listDatasets(VERTICATABLE)) { tableName ->
        // Copy rows from the embedded table to the Vertica table
        copyRows(embeddedTable(tableName), verticaTable(tableName)) {
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
