package getl.lang

import getl.exception.ExceptionGETL
import getl.test.TestRunner
import groovy.transform.BaseScript
import groovy.transform.Field

//noinspection GroovyUnusedAssignment
@BaseScript TestRunner main

void init() {
    configuration {
        load 'resource:/config/dsl_test_useconfig.conf'
        readFields 'init'
    }
}

void done() {
    configContent.doneScript = 'complete test 3'
}

void error(Exception e) {
    configContent.errorScript = "error test 3: $e.message".toString()
}

@Field Boolean useExtVars = false
@Field Integer param1 = 0; assert (!useExtVars && param1 == 1) || (useExtVars && param1 == 2)
@Field Boolean throwError = false

if (throwError)
    throw new ExceptionGETL("Throw error!")
