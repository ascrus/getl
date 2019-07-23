package getl.examples.hive

import getl.utils.FileUtils

@BaseScript getl.lang.Getl getl

import groovy.transform.BaseScript

/*
Configuration options

Create config file in <project path>/tests/hive/hive.dsl with syntax:
workPath = '<log and history files directory>'
driverPath = '<hive jdbc file path>'
vendor = '<jdbc vendor name>' // use apache, hortonworks or cloudera
connectDatabase = '<hive database name>'
connectHost = '<hive node host:port>' // default port 10000
login = '<hive user name>'
password = '<hive user password>' // if password required
hdfsHost = 'hdfs host'
hfdsPort = <hdfs port> // default 8022
hdfsLogin = '<hdfs login>'
hdfsDir = '<hdfs directory>'
*/
configuration {
    // Clear content configuration
    clear()

    // Directory of configuration file
    path = (FileUtils.FindParentPath('.', 'src/main/groovy/getl')?:'') + 'tests/hive'

    // Load configuration file
    load'hive.dsl'

    // Print message to log file and console
    logConfig "Load configuration hive.dsl complete. Use directory \"${configContent.workPath}\"."
}