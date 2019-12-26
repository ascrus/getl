package getl.lang

import groovy.transform.BaseScript
import groovy.transform.Field

//noinspection GroovyUnusedAssignment
@BaseScript Getl main

configContent.script_params = [param1: 1, param2: 'a', param3: [1,2,3], param4: [a: 1, b: 2, c: 3], param5: 'not found']
configuration {
    readFields 'script_params', false
}

@Field def param1 = 0; assert param1.toString() == '1'
@Field def param2; assert param2.toString() == 'a'
@Field def param3 = [0,0]; assert param3.toString() == '[1, 2, 3]'
@Field def param4 = [a:0]; assert param4.toString() == '[a:1, b:2, c:3]'

configContent.testScript = 'complete test 2'
