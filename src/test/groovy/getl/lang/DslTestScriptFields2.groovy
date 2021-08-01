package getl.lang

import getl.test.TestRunner
import groovy.transform.BaseScript
import groovy.transform.Field

//noinspection GroovyUnusedAssignment
@BaseScript TestRunner main

assert main.unitTestMode

@Field Integer param1 = 0
@Field String param2
@Field List param3 = [0,0]
@Field Map param4 = [a:0]
@Field String tableName

main.configContent.script_params = [param1: 1, param2: 'a', param3: [1,2,3], param4: [a: 1, b: 2, c: 3], param5: 'not found']
configuration {
    if (param1 == 0)
        readFields 'script_params', false
}

assert param1 == 1
assert param2 == 'a'
assert param3 == [1, 2, 3]
assert param4 == [a:1, b:2, c:3]

assert tableName != null && tableName == '#scripttable2'
assert embeddedTable(tableName).tableName == 'test_script_2'

cloneDataset('#scripttable2_new', embeddedTable(tableName))
assert !listDatasets('#scripttable2_new').isEmpty()

main.configContent.testScript = 'complete test 2'
