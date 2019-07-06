package getl.examples.utils

import getl.jdbc.JDBCConnection
import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

runGroovyClass getl.examples.h2.Install

thread(listDatasets(EMBEDDEDTABLE)) {
    run(2) {
        runGroovyClass ThreadProcessTable, [tableName: it]
    }
}