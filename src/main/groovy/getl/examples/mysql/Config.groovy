package getl.examples.mysql

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

/*
Configuration options

Create config file in <project path>/tests/mysql/mysql.dsl with syntax:
workPath = '<log and history files directory>'
driverPath = '<mysql jdbc file path>'
connectDatabase = '<mysql database name>'
connectHost = '<mysql node host>'
login = '<mysql user name>'
password = '<mysql user password>'
*/
configuration {
    // Clear content configuration
    clear()

    // Directory of configuration file
    path = (findParentPath('.', 'src/main/groovy/getl')?:'') + 'tests/mysql'

    // Load configuration file
    load'mysql.dsl'

    // Print message to log file and console
    logConfig "Load configuration mysql.dsl complete. Use directory \"${configContent.workPath}\"."
}