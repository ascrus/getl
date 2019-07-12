package getl.examples.h2

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

/**
 * Define H2 embedded tables
 */

embeddedTable('prices', true) { tableName = 'prices' }
embeddedTable('customers', true) { tableName = 'customers' }
embeddedTable('customers.phones', true) { tableName = 'customer_phones' }
embeddedTable('sales', true) { tableName = 'sales' }
