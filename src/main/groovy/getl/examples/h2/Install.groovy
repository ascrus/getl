package getl.examples.h2

import getl.utils.DateUtils
import getl.utils.GenerationUtils

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Count sale rows
def count_sale_rows = 250000

// Define H2 tables
runGroovyClass getl.examples.h2.Tables

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
            add id: 1, name: 'Apple', create_date: DateUtils.now, price: 60.50, description: 'Not a macintosh.\nThis is fruit.'
            add id: 2, name: 'Pear', create_date: DateUtils.now, price: 90.00, description: null
            add id: 3, name: 'Plum', create_date: DateUtils.now, price: 110.00, description: 'Not a Green Plum.\nThis is fruit.'
            add id: 4, name: 'Cherries', create_date: DateUtils.now, price: 150.10, description: 'Not a china machine.\nThis is fruit.'
            add id: 5, name: 'Melon', create_date: DateUtils.now, price: 30.00, description: null
            add id: 6, name: 'Blackberry', create_date: DateUtils.now, price: 70.90, description: 'Not a phone.\nThis is fruit.'
            add id: 7, name: 'Blueberries', create_date: DateUtils.now, price: 85.00, description: null
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
runGroovyClass getl.examples.xml.CustomersXML

// Copy customers rows from xml file to h2 tables customers and customers_phones
copyRows(xml('customers'), embeddedTable('customers')) {
    bulkLoad = true

    // Adding an write to the child table customers_phones
    childs('customers.phones', embeddedTable('customers.phones')) {
        // Processing the child structure phones
        processRow { addPhone, row ->
            // Copying phones array to the writer in h2 table phones customers
            row.phones?.each { phone ->
                addPhone customer_id: row.id, phone: phone?.text()
            }
        }
        childDone { logInfo "${dataset.updateRows} customer phones loaded" }
    }
    doneFlow { logInfo "${destination.updateRows} customers loaded" }
}

assert embeddedTable('customers').countRow() == 3
assert embeddedTable('customers.phones').countRow() == 7

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
                def price = GenerationUtils.GenerateInt(1, 7)
                def customer = GenerationUtils.GenerateInt(1, 3)
                def sale_date = GenerationUtils.GenerateDate(90)
                def count = GenerationUtils.GenerateInt(1, 99)
                def priceRow = priceLookup.get(price) as Map
                def sum = priceRow.price * count
                def desc = GenerationUtils.GenerateString(50)

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