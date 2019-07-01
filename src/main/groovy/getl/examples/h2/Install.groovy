package getl.examples.h2

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Count sale rows
def count_sale_rows = 250000

// DSL options
options {
    // Enabled process timing
    processTimeTracing = true
}

// Price table
embeddedTable('prices') { table ->
    tableName = 'prices'
    field('id') { type = integerFieldType; isKey = true }
    field('name') { type = stringFieldType; isNull = false; length = 50 }
    field('create_date') { type = datetimeFieldType; isNull = false }
    field('price') { type = numericFieldType; isNull = false; length = 9; precision = 2 }
    field('description') { type = textFieldType }

    create()
    logInfo "Created h2 table $table"

    logFine"Generating data to h2 table $table ..."
    rowsTo(table) {
        // User code
        process { add -> // writer object
            add id: 1, name: 'Apple', create_date: now, price: 60.50, description: 'Not a macintosh.\nThis is fruit.'
            add id: 2, name: 'Pear', create_date: now, price: 90.00, description: null
            add id: 3, name: 'Plum', create_date: now, price: 110.00, description: 'Not a Green Plum.\nThis is fruit.'
            add id: 4, name: 'Cherries', create_date: now, price: 150.10, description: 'Not a china machine.\nThis is fruit.'
            add id: 5, name: 'Melon', create_date: now, price: 30.00, description: null
            add id: 6, name: 'Blackberry', create_date: now, price: 70.90, description: 'Not a phone.\nThis is fruit.'
            add id: 7, name: 'Blueberries', create_date: now, price: 85.00, description: null
        }
        done {
            logInfo"$countRow rows saved to h2 table $table"
        }
    }
}

// Customers table
embeddedTable('customers') { table ->
    tableName = 'customers'
    field('id') { type = integerFieldType; isKey = true }
    field('name') { length = 50 }
    field('customer_type') { length = 10 }

    create()
    logInfo "Created h2 table $table"
}

// Customer phones table
embeddedTable('customers.phones') { table ->
    tableName = 'customer_phones'
    field('customer_id') { type = integerFieldType; isKey = true }
    field('phone') { length = 50; isKey = true }

    create()
    logInfo "Created h2 table $table"
}

// Load customers data from generated XML file
runGroovyScript 'getl.examples.xml.LoadXmlToH2'

// Sales table
embeddedTable('sales') { table ->
    tableName = 'sales'
    field('id') { type = bigintFieldType; isKey = true }
    field('price_id') { type = integerFieldType; isNull = false }
    field('customer_id') { type = integerFieldType; isNull = false }
    field('sale_date') { type = datetimeFieldType; isNull = false }
    field('sale_count') { type = bigintFieldType; isNull = false }
    field('sale_sum') { type = numericFieldType; isNull = false; length = 12; precision = 2 }
    field('description') { type = stringFieldType; length = 50 }

    create()
    logInfo "Created h2 table $table"

    logFine"Generating data to h2 table $table ..."
    rowsTo(table) {
        // Lookup price map structure
        def priceLookup = embeddedTable('prices').lookup { key = 'id'; strategy = ORDER_STRATEGY }

        // Size of batch saving rows
        destParams.batchSize = 10000

        // User code
        process { add -> // writer object
            def num = 0
            (1..count_sale_rows).each { id ->
                num++
                def price = randomInt(1, 7)
                def customer = randomInt(1, 3)
                def sale_date = randomDate(90)
                def count = randomInt(1, 99)
                def priceRow = priceLookup.get(price) as Map
                def sum = priceRow.price * count
                def desc = randomString(50)

                // Append row to destination
                add id: num, price_id: price, customer_id: customer, sale_date: sale_date, sale_count: count, sale_sum: sum,
                        description: desc
            }
        }
        done {
            logInfo"$countRow rows saved to h2 table $table"
        }
    }
}