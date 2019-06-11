package getl.examples.vertica

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Count sale rows
def count_sale_rows = 100000

// Generate data in temporary database
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

    processRepDatasets(VERTICATABLE) {
        verticaTable(it) { table ->
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

// Copy rows of pricing list from the embedded table to the Vertica table
copyRows(embeddedTable('price'), verticaTable('price')) { source, dest ->
    done { logInfo "Copied $countRow rows of pricing list from the embedded table to the Vertica table" }
}

// Copy rows of sales fact from the embedded table to the Vertica table
copyRows(embeddedTable('sales'), verticaTable('sales')) { source, dest ->
    bulkLoad = true
    done { logInfo "Copied $countRow rows of sales fact from the embedded table to the Vertica table" }
}