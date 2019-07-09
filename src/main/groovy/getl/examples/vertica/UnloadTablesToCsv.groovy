package getl.examples.vertica

import getl.utils.DateUtils
import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

/**
 * Unload all rows from Vertica tables to Csv files
 * <br>(sales data will be unloaded with month partitioning)
 */

// Load configuration file
runGroovyClass getl.examples.vertica.Config
// Define Vertica tables
runGroovyClass getl.examples.vertica.Tables
// Define Csv files
runGroovyClass getl.examples.vertica.CsvFiles

thread {
    run(listDatasets(VERTICATABLE) - ['sales']) { tableName ->
        copyRows(verticaTable(tableName), csv(tableName)) {
            done {
                logInfo "$countRow rows copied from Vertica table $source to $destination"
            }
        }
    }
}

thread {
    run(sqlQuery(
            "SELECT DISTINCT Trunc(sale_date, 'month')::date as month FROM getl_demo.sales ORDER BY month").rows(),
            3) { sale_day ->
        def sales = verticaTable('sales') {
            readOpts {
                where = "Trunc(sale_date, 'month')::date = '{month}'::date"
            }
            queryParams.month = sale_day.month
        }
        def file = csv('sales') {
            fileName = fileName + '.' + formatDate(sale_day.month, 'yyyyMMdd')
        }

        copyRows(sales, file) {
            done {
                logInfo "$countRow rows copied from Vertica table $source to $destination"
            }
        }
    }
}
