package getl.examples.h2

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

/**
 * Generate new sales and append to table
 */

def countAppend = 10000

// Define H2 tables
runGroovyClass getl.examples.h2.Tables

// Get max id from sales
def maxSales = sqlQueryRow('SELECT Max(id) AS max_id FROM {table}',
        [table: embeddedTable('sales').fullTableName])
assert maxSales != null, 'Table sales is empty!'

rowsTo(embeddedTable('sales')) {
    process { addSale ->
        ((maxSales.max_id + 1)..(maxSales.max_id + countAppend)).each {

        }
    }
}