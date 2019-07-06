package getl.examples.oracle

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

/*
Configuration options

Create config file in <project path>/tests/oracle/oracle.dsl with syntax:
workPath = '<log and history files directory>'
driverPath = '<oracle jdbc file path>'
connectDatabase = '<oracle database name>'
connectHost = '<oracle node host>'
login = '<oracle user name>'
password = '<oracle user password>'
*/
configuration {
    // Clear content configuration
    clear()

    // Directory of configuration file
    path = (findParentPath('.', 'src/main/groovy/getl')?:'') + 'tests/oracle'

    // Load configuration file
    load'oracle.dsl'

    // Print message to log file and console
    logConfig "Load configuration oracle.dsl complete. Use directory \"${configContent.workPath}\"."
}