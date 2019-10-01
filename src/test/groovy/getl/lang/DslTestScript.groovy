package getl.lang

import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript Getl main

@Field Integer param1 = 0; assert param1 == 1
@Field def param2; assert param2 == 2
@Field def param4 = '123'; assert param4 == '123'
@Field def param5 = [0,0]; assert param5 == [1,2,3]
@Field def param6 = [a:0]; assert param6 == [a:1, b:2, c:3]
@Field int paramCountTableRow; assert paramCountTableRow != null

assert scriptArgs.param3 == 3
