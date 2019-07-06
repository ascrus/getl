package getl.examples.mysql

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Generate sample data in a H2  database
runGroovyClass getl.examples.h2.Install

// Load configuration file
runGroovyClass getl.examples.mysql.Config
// Define object as MySQL tables
runGroovyClass getl.examples.mysql.Tables

profile("Create MySQL objects") {
    // Run sql script for create schemata and tables
    sql {
        exec 'CREATE SCHEMA IF NOT EXISTS getl_demo;'
        logInfo'Created schema getl_demo.'
    }

    processDatasets(MYSQLTABLE) { tableName ->
        mysqlTable(tableName) { table ->
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

thread(listDatasets(MYSQLTABLE)) {
    run { tableName ->
        // Copy rows from the embedded table to the MySQL table
        copyRows(embeddedTable(tableName), mysqlTable(tableName)) { source, dest ->
            done { logInfo "Copied $countRow rows of $tableName from the embedded table to the MySQL table" }
        }
    }
}

thread {
    addThread {
        assert mysqlTable('prices').countRow() == 7
    }
    addThread {
        assert mysqlTable('customers').countRow() == 3
    }
    addThread {
        assert mysqlTable('customers.phones').countRow() == 7
    }
    addThread {
        assert mysqlTable('sales').countRow() == 250000
    }

    exec()
}
