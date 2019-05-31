@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Vertica database connection (using config variables)
useJDBCConnection verticaConnection('demo') {
    driverPath = configVars.driverPath
    connectHost = configVars.connectHost
    connectDatabase = configVars.connectDatabase
    schemaName = 'getl_demo'
    login = configVars.login
    password = configVars.password
    sqlHistoryFile = "${configVars.workPath}/vertica.{date}.sql"
}

// Vertica price table
verticatable('price') {
    tableName = 'price'
    field('id') { type = integerFieldType; isKey = true }
    field('name') { type = stringFieldType; isNull = false; length = 50 }
    field('create_date') { type = datetimeFieldType; isNull = false }
    field('price') { type = numericFieldType; isNull = false; length = 9; precision = 2 }
    field('description') { type = textFieldType }
}

// Vertica sales table
verticatable('sales') {
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

processRepDatasets(VERTICATABLE) { tableName ->
    verticatable(tableName) {
        // Define csv temp table from this table
        csvTempWithDataset(tableName, it)
    }
}