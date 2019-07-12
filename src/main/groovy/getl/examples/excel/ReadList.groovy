package getl.examples.excel

import getl.data.Field
import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

excel { dataset ->
    connection = excelConnection {
        path = (findParentPath('.', 'src/main/groovy/getl')?:'') + 'tests/excel'
        fileName = 'test.xlsx'
        listName = 'test'
        header = true
    }

    copyRows(dataset, csvTempWithDataset('demo', dataset))

    assert field('a').type == Field.numericFieldType
    assert field('b').type == Field.numericFieldType
    assert field('c').type == Field.stringFieldType
}

rowProcess(csvTemp('demo')) {
    process { logInfo(it) }
}