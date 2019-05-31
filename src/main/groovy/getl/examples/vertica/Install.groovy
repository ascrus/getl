@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Count sale rows
def count_sale_rows = 100000

// Execute option setting script
runGroovyFile 'src/main/groovy/getl/examples/vertica/Options.groovy'
// Execute definitation Vertica table script
runGroovyFile 'src/main/groovy/getl/examples/vertica/Tables.groovy'

profile("Create Vertica objects") {
    // Run sql script for create schemata and tables
    sql {
        exec 'CREATE SCHEMA IF NOT EXISTS getl_demo;'
        logInfo'Created schema getl_demo.'
    }

    processRepDatasets(VERTICATABLE) { tableName ->
        verticatable(tableName) {
            // Create table in database
            create ifNotExists: true
            logInfo"Created table $it."
        }
    }
}

logFine'Generating data to price temporary file...'
// Write specified rows to Vertica table price
rowsTo(csvTemp('price')) {
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
        logInfo"$countRow price rows saved to temporary table $it"
    }
}
// Load temporary file to price Vertica table
copyRows(csvTemp('price'), verticatable('price'))


logFine'Generate date to sales temporary file ...'
// Generate data
rowsTo(csvTemp('sales')) {
    // Lookup price map structure
    def priceLookup = csvTemp('price').lookup { key = 'id'; strategy = ORDER_STRATEGY }

    // User code
    process { add -> // writer object
        (1..count_sale_rows).each { id ->
            def price = randomInt(1, 7)
            def count = randomInt(1, 99)
            def priceRow = priceLookup.get(price) as Map
            def sum = priceRow.price * count

            // Append row to destination
            add id: 1, price_id: price, sale_date: randomDate(90), sale_count: count, sale_sum: sum
        }
    }
    done {
        logInfo"$countRow rows saved to $it"
    }
}
// Load temporary file to sales Vertica table
verticaBulkLoad('sales') { it.truncate() }