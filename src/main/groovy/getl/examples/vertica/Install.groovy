package getl.examples.vertica

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Count sale rows
def count_sale_rows = 100000

// Generate sample data in a H2  database
runGroovyScript 'getl.examples.h2.Install'

// Load configuration file
runGroovyScript 'Config'
// Define object as Vertica tables
runGroovyScript 'Tables'

profile("Create Vertica objects") {
    // Run sql script for create schemata and tables
    sql {
        exec 'CREATE SCHEMA IF NOT EXISTS getl_demo;'
        logInfo'Created schema getl_demo.'
    }

    processRepDatasets(VERTICATABLE) { tableName ->
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

thread(listRepDatasets(VERTICATABLE)) {
    countProc = 3
    run { tableName ->
        // Copy rows from the embedded table to the Vertica table
        copyRows(embeddedTable(tableName), verticaTable(tableName)) { source, dest ->
            bulkLoad = true
            done { logInfo "Copied $countRow rows of $tableName from the embedded table to the Vertica table" }
        }
    }
}