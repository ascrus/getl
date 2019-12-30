package getl.lang

import groovy.transform.BaseScript
import groovy.transform.Field

//noinspection GroovyUnusedAssignment
@BaseScript Getl main

configContent.script_params = [param1: 1, param2: 'a', param3: [1,2,3], param4: [a: 1, b: 2, c: 3], param5: 'not found']
configuration {
    readFields 'script_params', false
}

@Field Integer param1 = 0; assert param1 == 1
@Field String param2; assert param2 == 'a'
@Field List param3 = [0,0]; assert param3 == [1, 2, 3]
@Field Map param4 = [a:0]; assert param4 == [a:1, b:2, c:3]

configContent.testScript = 'complete test 2'
