package getl.examples.vertica

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

// Load configuration file
runGroovyClass getl.examples.vertica.Config, true

/**
 * Define Vertica connection and tables
 */

// Vertica database connection (using config content variables)
useVerticaConnection verticaConnection('demo', true) {
    driverPath = configContent.driverPath
    connectHost = configContent.connectHost
    connectDatabase = configContent.connectDatabase
    schemaName = 'getl_demo'
    login = configContent.login
    password = configContent.password
    sqlHistoryFile = "${configContent.workPath}/vertica.{date}.sql"
}

historypoint('vertica.demo', true) {
    schemaName = connection.schemaName
    tableName = 'demo_history_point'
    saveMethod = insertSave
}

// Price table
verticaTable('prices', true) {
    tableName = 'prices'
    field('id') { type = integerFieldType; isKey = true }
    field('name') { type = stringFieldType; isNull = false; length = 50 }
    field('create_date') { type = datetimeFieldType; isNull = false }
    field('price') { type = numericFieldType; isNull = false; length = 9; precision = 2 }
    field('description') { type = textFieldType }
}

// Customers table
verticaTable('customers', true) { table ->
    tableName = 'customers'
    field('id') { type = integerFieldType; isKey = true }
    field('name') { length = 50 }
    field('customer_type') { length = 10 }
}

// Customer phones table
verticaTable('customers.phones', true) { table ->
    tableName = 'customer_phones'
    field('customer_id') { type = integerFieldType; isKey = true }
    field('phone') { length = 50; isKey = true }
}

// Sales table
verticaTable('sales', true) {
    tableName = 'sales'
    field('id') { type = bigintFieldType; isKey = true }
    field('price_id') { type = integerFieldType; isNull = false }
    field('customer_id') { type = integerFieldType; isNull = false }
    field('sale_date') { type = datetimeFieldType; isNull = false }
    field('sale_count') { type = bigintFieldType; isNull = false }
    field('sale_sum') { type = numericFieldType; isNull = false; length = 12; precision = 2 }
    createOpts {
        orderBy = ['sale_date', 'price_id', 'customer_id']
        segmentedBy = 'hash(id) all nodes'
    }
}