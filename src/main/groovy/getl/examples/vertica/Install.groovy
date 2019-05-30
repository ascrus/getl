@BaseScript getl.lang.Getl getl

import getl.csv.CSVDataset
import getl.lang.opts.BaseSpec
import getl.utils.GenerationUtils
import getl.utils.StringUtils
import getl.vertica.VerticaTable
import groovy.transform.BaseScript

// Count sale rows
def count_sale_rows = 100000

runGroovyFile 'src/main/groovy/getl/examples/vertica/Options.groovy'

// Set flag for create table in Vertica
configVars.createTable = true
runGroovyFile 'src/main/groovy/getl/examples/vertica/Tables.groovy'

// Create table in temporary database with price vertica table
tempDBTableWithDataset('price', verticatable('price')) { table ->
    logFine'Generating data for price ...'
    // Write specified rows to table
    rowsTo {
        dest = table

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
            logInfo("$countRow rows saved to temporary table $table")
        }
    }
}

// Start multithread mode
thread {
    // Thread code load data to price vertica table
    addThread {
        logFine'Save date to price table ...'

        copyRows {
            source = tempDBTable('price')
            dest = verticatable('price')
            clear = true
            done { logInfo("$countRow rows load from table $dest") }
        }
    }

    // Thread code load data to sales vertica table
    addThread {
        logFine'Save date to sales table ...'

        // Temp sale file
        csvTemp('sales') { file ->
            // Generate data
            rowsTo {
                dest = file

                // Lookup price map structure
                def priceLookup = tempDBTable('price').lookup { key = 'id'; strategy = ORDER_STRATEGY }

                // User code
                process { add -> // writer object
                    (1..count_sale_rows).each { id ->
                        def price = randomInt(1, 7)
                        def count = randomInt(1, 99)
                        def priceRow = priceLookup.get(price) as Map
                        def sum = priceRow.price * count

                        add id: 1, price_id: price, sale_date: randomDate(90), sale_count: count, sale_sum: sum
                    }
                }
                done {
                    logInfo("$countRow rows saved to $file")
                }
            }
        }

        verticatable('sales') { table ->
            truncate()
            bulkLoadOpts { source = csvTemp('sales') }
            bulkLoadFile()
            logInfo("${table.countRows()} rows load to table $table")
        }
    }

    exec()
}