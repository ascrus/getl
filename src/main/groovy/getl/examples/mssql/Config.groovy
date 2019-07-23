package getl.examples.mssql

import getl.utils.FileUtils

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

/*
Configuration options

Create config file in <project path>/tests/mssql/mssql.dsl with syntax:
workPath = '<log and history files directory>'
driverPath = '<mssql jdbc file path>'
connectDatabase = '<mssql database name>'
connectHost = '<mssql node host>'
login = '<mssql user name>'
password = '<mssql user password>'
*/
configuration {
    // Clear content configuration
    clear()

    // Directory of configuration file
    path = (FileUtils.FindParentPath('.', 'src/main/groovy/getl')?:'') + 'tests/mssql'

    // Load configuration file
    load'mssql.dsl'

    // Print message to log file and console
    logConfig "Load configuration mssql.dsl complete. Use directory \"${configContent.workPath}\"."
}