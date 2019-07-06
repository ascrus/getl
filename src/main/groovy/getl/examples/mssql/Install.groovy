package getl.examples.mssql

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Generate sample data in a H2  database
runGroovyClass getl.examples.h2.Install

// Load configuration file
runGroovyClass getl.examples.mssql.Config
// Define object as Oracle tables
runGroovyClass getl.examples.mssql.Tables

profile("Create MSSQL objects") {
    // Run sql script for create schemata and tables
    sql {
        exec """
BEGIN BLOCK;
IF schema_id('getl_demo') IS NULL
BEGIN 
    EXEC sp_executesql N'CREATE SCHEMA getl_demo'
    COMMIT
END;
END BLOCK;"""
        logInfo'Created schema getl_demo.'
    }

    processDatasets(MSSQLTABLE) { tableName ->
        mssqlTable(tableName) { table ->
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

thread(listDatasets(MSSQLTABLE)) {
    run { tableName ->
        // Copy rows from the embedded table to the Oracle table
        copyRows(embeddedTable(tableName), mssqlTable(tableName)) { source, dest ->
            done { logInfo "Copied $countRow rows of $tableName from the embedded table to the MSSQL table" }
        }
    }
}

thread {
    addThread {
        assert mssqlTable('prices').countRow() == 7
    }
    addThread {
        assert mssqlTable('customers').countRow() == 3
    }
    addThread {
        assert mssqlTable('customers.phones').countRow() == 7
    }
    addThread {
        assert mssqlTable('sales').countRow() == 250000
    }

    exec()
}
