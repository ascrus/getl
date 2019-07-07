package getl.examples.postgresql

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

/*
Configuration options

Create config file in <project path>/tests/postgresql/postgresql.dsl with syntax:
workPath = '<log and history files directory>'
driverPath = '<postgresql jdbc file path>'
connectDatabase = '<postgresql database name>'
connectHost = '<postgresql node host>'
login = '<postgresql user name>'
password = '<postgresql user password>'
*/
configuration {
    // Clear content configuration
    clear()

    // Directory of configuration file
    path = (findParentPath('.', 'src/main/groovy/getl')?:'') + 'tests/postgresql'

    // Load configuration file
    load'postgresql.dsl'

    // Print message to log file and console
    logConfig "Load configuration postgresql.dsl complete. Use directory \"${configContent.workPath}\"."
}