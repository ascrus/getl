package getl.lang

import getl.tfs.TFS
import groovy.transform.BaseScript

@BaseScript Getl main

/** Temporary path */
final def tempPath = TFS.systemPath
/** Config file name */
final def tempConfig = "$tempPath/getl.conf"
/** H2 table name */
final def h2TableName = 'table1'
/** CSV file name 1 */
final def csvFileName1 = 'file1.csv'
/** CSV file name 2 */
final def csvFileName2 = 'file2.csv'
/** Count rows in table1 */
final def table1_rows = 100

logInfo "Use temporary path: ${tempPath}"

// Generate configuration file
textFile(this.tempConfig) {
    temporaryFile = true
    write """
datasets {
    table1 {
        tableName = '${this.h2TableName}'
    }
    
    file1 {
        fileName = '${this.csvFileName1}'
    }
    
    file2 {
        fileName = '${this.csvFileName2}'
    }
}
"""
}

// Load configuration
configuration {
    path = this.tempPath
    load('getl.conf')
}