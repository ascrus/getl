package getl.examples.vertica

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

// Define Vertica tables
runGroovyClass getl.examples.vertica.Tables, true
// Define Csv files
runGroovyClass getl.examples.vertica.CsvFiles, true

files {
    rootPath = csvConnection('demo').path
    registerDataset buildListFiles('{table}.csv.gz'), 'csv_files'
}