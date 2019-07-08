package getl.examples.hive

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Hive database connection (using config content variables)
useHiveConnection hiveConnection('demo') {
    driverPath = configContent.driverPath
    vendor = configContent.vendor
    connectHost = configContent.connectHost
    connectDatabase = configContent.connectDatabase
    schemaName = 'getl_demo'
    login = configContent.login
    password = configContent.password
    hdfsHost = configContent.hdfsHost
    hdfsPort = configContent.hdfsPort
    hdfsDir = configContent.hdfsDir
    hdfsLogin = configContent.hdfsLogin
    sqlHistoryFile = "${configContent.workPath}/hive.{date}.sql"
}

// Price table
hiveTable('prices') {
    tableName = 'prices'
    field('id') { type = integerFieldType; isKey = true }
    field('name') { type = stringFieldType; isNull = false; length = 50 }
    field('create_date') { type = datetimeFieldType; isNull = false }
    field('price') { type = numericFieldType; isNull = false; length = 9; precision = 2 }
    field('description') { type = stringFieldType; length = 8000 }

    createOpts {
        storedAs = 'ORC'
        clustered {
            by = ['id']
            intoBuckets = 2
        }
        tblproperties.transactional = true
    }
}

// Customers table
hiveTable('customers') { table ->
    tableName = 'customers'
    field('id') { type = integerFieldType; isKey = true }
    field('name') { length = 50 }
    field('customer_type') { length = 10 }

    createOpts {
        storedAs = 'ORC'
        clustered {
            by = ['id']
            intoBuckets = 2
        }
        tblproperties.transactional = true
    }
}

// Customer phones table
hiveTable('customers.phones') { table ->
    tableName = 'customer_phones'
    field('customer_id') { type = integerFieldType; isKey = true }
    field('phone') { length = 50; isKey = true }

    createOpts {
        storedAs = 'ORC'
        clustered {
            by = ['customer_id']
            intoBuckets = 2
        }
        tblproperties.transactional = true
    }
}

// Sales table
hiveTable('sales') {
    tableName = 'sales'
    field('id') { type = bigintFieldType; isKey = true }
    field('price_id') { type = integerFieldType; isNull = false }
    field('customer_id') { type = integerFieldType; isNull = false }
    field('sale_date') { type = datetimeFieldType; isNull = false }
    field('sale_count') { type = bigintFieldType; isNull = false }
    field('sale_sum') { type = numericFieldType; isNull = false; length = 12; precision = 2 }
    createOpts {
        storedAs = 'ORC'
        clustered {
            by = ['id']
            intoBuckets = 2
        }
        tblproperties.transactional = true
    }
}