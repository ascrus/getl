package getl.examples.utils

import getl.lang.Getl
import groovy.transform.BaseScript

@BaseScript Getl getl

logInfo "Table ${scriptArgs.tableName} has " + embeddedTable(scriptArgs.tableName).countRow() + " rows."