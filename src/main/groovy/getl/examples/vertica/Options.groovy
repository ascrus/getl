@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

/*
Configuration options

Create config file in ./tests/vertica/vertica.dsl with syntax:
vars {
    workPath = '<log and history files directory>'
    driverPath = '<vertica jdbc file path>'
    connectDatabase = '<vertica database name>'
    connectHost = '<vertica node host>'
    login = '<vertica user name>'
    password = '<vertica user password>'
    ssh_login = '<ssh login on Vertica host>
    ssh_password = '<ssh password on Vertica host>
    ssh_rsakey = '<ssh rsa string key for host>' // use "ssh-keyscan -t rsa <host-name>"
}
*/
config {
    // Directory of configuration file
    path = configVars.configPath?:'tests/vertica'

    // Load configuration file
    load'vertica.dsl'

    // Print message to log file and console
    logConfig "Load configuration vertica.dsl complete. Use directory \"${configVars.workPath}\"."
}

// Logger options
log {
    // Log file name with {date} variable in name
    logFileName = "${configVars.workPath}/vertica.{date}.log"
}

// DSL options
options {
    // Enabled process timing
    processTimeTracing = true

    // Set multithread connection model
    useThreadModelJDBCConnection = true

    // Save sql from temp database to file
    tempDBSQLHistoryFile = "${configVars.workPath}/tempdb.{date}.sql"
}