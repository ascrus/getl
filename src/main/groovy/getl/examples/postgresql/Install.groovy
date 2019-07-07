package getl.examples.postgresql

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Generate sample data in a H2  database
runGroovyClass getl.examples.h2.Install

// Load configuration file
runGroovyClass getl.examples.postgresql.Config
// Define object as PostgreSQL tables
runGroovyClass getl.examples.postgresql.Tables

options {
    // Enabled chech on exists objects
    validObjectExist = true
}

profile("Create PostgreSQL objects") {
    // Run sql script for create schemata and tables
    sql {
        exec 'CREATE SCHEMA IF NOT EXISTS getl_demo;'
        logInfo'Created schema getl_demo.'
    }

    processDatasets(POSTGRESQLTABLE) { tableName ->
        postgresqlTable(tableName) { table ->
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

thread(listDatasets(POSTGRESQLTABLE)) {
    run { tableName ->
        // Copy rows from the embedded table to the PostgreSQL table
        copyRows(embeddedTable(tableName), postgresqlTable(tableName)) { source, dest ->
            done { logInfo "Copied $countRow rows of $tableName from the embedded table to the PostgreSQL table" }
        }
    }
}

thread {
    addThread {
        assert postgresqlTable('prices').countRow() == 7
    }
    addThread {
        assert postgresqlTable('customers').countRow() == 3
    }
    addThread {
        assert postgresqlTable('customers.phones').countRow() == 7
    }
    addThread {
        assert postgresqlTable('sales').countRow() == 250000
    }

    exec()
}
