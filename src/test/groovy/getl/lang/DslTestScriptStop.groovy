package getl.lang

import getl.test.TestRunner
import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript TestRunner main

@Field Short level

configContent.test_stop = false

if (level == 1) {
    runGroovyClass getl.lang.DslTestScriptStop, { level = 2 }
    assert 0 == 1
}
else if (level == 2) {
    def res = runGroovyClass getl.lang.DslTestScriptStop, { level = 3 }
    assert res == 3
    configContent.test_stop = true
    appRunSTOP(2)
    assert 0 == 1
}
else {
    classRunSTOP('Stop class!', 3)
    assert 0 == 1
}

configContent.test_stop = false