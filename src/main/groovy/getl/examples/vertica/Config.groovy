package getl.examples.vertica

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

/**
 * Load configuration file
 */

/*
Configuration options

Create config file in <project path>/tests/vertica/vertica.dsl with syntax:
workPath = '<csv, log and history files directory>'
driverPath = '<vertica jdbc file path>'
connectDatabase = '<vertica database name>'
connectHost = '<vertica node host>'
login = '<vertica user name>'
password = '<vertica user password>'
ssh_login = '<ssh login on Vertica host>'
ssh_password = '<ssh password on Vertica host>'
ssh_rsakey = '<ssh rsa string key for host>' // use "ssh-keyscan -t rsa <host-name>"
*/
configuration {
    // Clear content configuration
    clear()

    // Directory of configuration file
    path = (findParentPath('.', 'src/main/groovy/getl')?:'') + 'tests/vertica'

    // Load configuration file
    load'vertica.dsl'

    // Print message to log file and console
    logConfig "Load configuration vertica.dsl complete. Use directory \"${configContent.workPath}\"."
}