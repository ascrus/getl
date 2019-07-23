package getl.examples.vertica

import getl.utils.DateUtils
import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

/**
 * Unload all rows from Vertica tables to Csv files
 * <br>(sales data will be unloaded with month partitioning)
 */

// Define Vertica tables
runGroovyClass getl.examples.vertica.Tables, true
// Define Csv files
runGroovyClass getl.examples.vertica.CsvFiles, true

// Unload data from all Vertica tables without the sales table
thread {
    run(listDatasets(VERTICATABLE) - ['sales']) { tableName ->
        copyRows(verticaTable(tableName), csv(tableName)) {
            done {
                logInfo "$countRow rows copied from Vertica table $source to $destination"
            }
        }
    }
}

// Unload data from the sales tables with month partition
thread {
    list = sqlQuery( // Get list of month sales partition
            "SELECT DISTINCT Trunc(sale_date, 'month')::date as month FROM getl_demo.sales ORDER BY month").rows()
    logInfo "Found ${list.size()} month partitions from sales table"
    run(3) { sale_day -> // Run thread on one month partition
        def sales = verticaTable('sales') { // Modify sales table definition (set filter)
            readOpts {
                where = "Trunc(sale_date, 'month')::date = '{month}'::date"
            }
            queryParams.month = sale_day.month
        }
        def file = csv('sales') { // Modify sales file definition (set file name)
            fileName = fileName + '.' + DateUtils.FormatDate('yyyyMMdd', sale_day.month)
        }

        copyRows(sales, file) { // Copy rows from one month partition to one csv file
            done {
                logInfo "$countRow rows copied from Vertica table $source to $destination"
            }
        }
    }
}
