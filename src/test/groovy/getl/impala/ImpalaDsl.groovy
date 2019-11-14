package getl.impala

import getl.lang.Getl
import getl.utils.DateUtils
import groovy.transform.BaseScript

@BaseScript Getl main

configuration {
    load '../../../../../tests/impala/impala.groovy'
}

useImpalaConnection impalaConnection('con', true) {
    useConfig 'impala'
    connected = true
}

impalaTable('tab', true) {
    tableName = 'getl_test_impala'
    field('id') { type = bigintFieldType }
    field('name')
    field('dt') { type = datetimeFieldType}
    field('flag') { type = booleanFieldType }
    field('double') { type = doubleFieldType }
    field('value') { type = numericFieldType; length = 12; precision = 2 }
    drop(ifExists: true)
    create(storedAs: 'PARQUET', sortBy: ['id'], tblproperties: [transactional: false])
}

rowsTo(impalaTable('tab')) {
    writeRow() { add ->
        (1..3).each { num ->
            add id: num, name: "name $num", dt: DateUtils.Now(), flag: true, double: Float.valueOf('123.45'), number: new BigDecimal('123.45')
        }
    }
}

rowProcess(impalaTable('tab')) {
    readRow() { row ->
        println row
    }
}
