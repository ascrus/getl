package getl.examples.vertica

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

/**
 * Define CSV files for load and unload data from Vertica tables
 */

// Connection for Csv files
useCsvConnection csvConnection('demo', true) {
    fieldDelimiter = '\t' // field delimiter char
    escaped = true // used escape coding for " and \n characters
    codePage = 'UTF-8' // write as utf-8 code page
    isGzFile = true // pack files to GZ
    path = "${configContent.workPath}/csv" // path for csv files
    extension = 'csv'
    createPath = true // create path if not exist
    autoSchema = true // create schema files for csv files
}

processDatasets(VERTICATABLE) { tableName -> // Process Vertica table
    csvWithDataset(tableName, verticaTable(tableName), true) { // Define Csv dataset for default connection on Vertica table base
        fileName = tableName
    }
}