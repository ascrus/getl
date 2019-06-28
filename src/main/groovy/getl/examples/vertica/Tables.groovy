@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Vertica database connection (using config content variables)
useJDBCConnection verticaConnection('demo') {
    driverPath = configContent.driverPath
    connectHost = configContent.connectHost
    connectDatabase = configContent.connectDatabase
    schemaName = 'getl_demo'
    login = configContent.login
    password = configContent.password
    sqlHistoryFile = "${configContent.workPath}/vertica.{date}.sql"
}

// Vertica price table
verticaTable('prices') {
    tableName = 'prices'
    field('id') { type = integerFieldType; isKey = true }
    field('name') { type = stringFieldType; isNull = false; length = 50 }
    field('create_date') { type = datetimeFieldType; isNull = false }
    field('price') { type = numericFieldType; isNull = false; length = 9; precision = 2 }
    field('description') { type = textFieldType }
}

// Vertica sales table
verticaTable('sales') {
    tableName = 'sales'
    field('id') { type = bigintFieldType; isKey = true }
    field('price_id') { type = integerFieldType; isNull = false }
    field('sale_date') { type = datetimeFieldType; isNull = false }
    field('sale_count') { type = bigintFieldType; isNull = false }
    field('sale_sum') { type = numericFieldType; isNull = false; length = 12; precision = 2 }
    createOpts {
        orderBy = ['sale_date', 'price_id']
        segmentedBy = 'hash(id) all nodes'
    }
}