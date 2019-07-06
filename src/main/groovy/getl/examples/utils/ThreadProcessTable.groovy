package getl.examples.utils

import getl.lang.Getl
import groovy.transform.BaseScript

@BaseScript Getl getl

def table = tableName

logInfo "Table $table:" + embeddedTable(table).countRow()