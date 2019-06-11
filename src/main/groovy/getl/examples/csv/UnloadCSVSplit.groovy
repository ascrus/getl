package getl.examples.csv

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

runGroovyScript 'getl.examples.h2.Install'

csvTempWithDataset('sales', embeddedTable('sales')) { file ->
    readOpts {
        isSplit = true
        isValid = false
    }
    writeOpts {
        def cur = 0
        splitFile { cur++; cur.mod(50000) == 0 }
    }

    copyRows(embeddedTable('sales'), file)
    logInfo "$writeRows write to $file with $countWritePortions partition ($countWriteCharacters characters)"

    rowProcess(file) {
        process { assert it.price_id in (1..7) }
        done { logInfo "$countRow read from $file with ${file.countReadPortions} partition" }
    }
}