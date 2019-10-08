package getl.lang

import groovy.transform.BaseScript
import groovy.transform.Field

//noinspection GroovyUnusedAssignment
@BaseScript Getl main

@Field def param1 = 0; assert param1.toString() == '1'
@Field def param2; //noinspection GroovyVariableNotAssigned
assert param2.toString() == '2'
@Field def param4 = '123'; assert param4.toString() == '123'
@Field def param5 = [0,0]; assert param5.toString() == '[1, 2, 3]'
@Field def param6 = [a:0]; assert param6.toString() == '[a:1, b:2, c:3]'
@Field def paramCountTableRow; assert paramCountTableRow != null

configContent.testScript = param1
