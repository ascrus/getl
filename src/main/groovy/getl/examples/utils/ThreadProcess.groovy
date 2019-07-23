package getl.examples.utils

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

runGroovyClass getl.examples.h2.Install

thread {
    run(listDatasets(EMBEDDEDTABLE), 2) {
        runGroovyClass ThreadProcessTable, [tableName: it]
    }
}