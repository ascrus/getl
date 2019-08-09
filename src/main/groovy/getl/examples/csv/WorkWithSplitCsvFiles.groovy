/**
 *  This example shows how to write data divided into portions of files and then read such files as a single data set.
 */

package getl.examples.csv

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

// Generate sample data in a H2  database
runGroovyClass getl.examples.h2.Install

// Create csv temporary dataset based on sales
csvTempWithDataset('sales', embeddedTable('sales')) { file ->
    // File reading options
    readOpts {
        isSplit = true // Reads a chunked file
        isValid = false // Check constraints when reading a file
    }

    // File writing options
    writeOpts {
        def cur = 0
        splitFile { cur++; cur % 50000 == 0 } // The definition that the data record should be moved to the next file
    }

    // Copy sample data from table to file
    copyRows(embeddedTable('sales'), file)
    logInfo "$writeRows write to $file with $countWritePortions partition ($countWriteCharacters characters)"

    // Get a list of recorded files by mask
    def files = fileConnection.listFiles {
        // Use file mask type
        type = fileType
        // Use sort by name
        sort = nameSort
        // Use file name mask plus chunk number
        mask = "${file.fileName}[.][0-9]+"
    }
    logInfo "Detected ${files.collect { File f -> f.name + '(' + f.size() + ' bytes)' }} files"
    assert files.size() == countWritePortions

    // Read split data files
    rowProcess(file) {
        process { assert it.price_id in (1..7) }
        done { logInfo "$countRow read from $file with ${file.countReadPortions} partition" }
    }

    assert countReadPortions == countWritePortions
}