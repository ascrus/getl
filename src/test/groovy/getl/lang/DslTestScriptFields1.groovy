package getl.lang

import getl.utils.DateUtils
import groovy.transform.BaseScript
import groovy.transform.Field

//noinspection GroovyUnusedAssignment
@BaseScript Getl main

testCase {
    assertTrue(main.testCaseMode)
}

@Field Integer param1 = 0; assert param1 == 1
@Field BigDecimal param2; assert param2 == 123.45
@Field String param4 = '123'; assert param4 == '123'
@Field List param5 = [0,0]; assert param5 == [1, 2, 3]
@Field Map param6 = [a:0]; assert param6 == [a:1, b:2, c:3]
@Field Date param7; assert DateUtils.ClearTime(param7) == DateUtils.ClearTime(new Date())
@Field Date param8; assert DateUtils.TruncTime('HOUR', param8) == DateUtils.TruncTime('HOUR', new Date())
@Field Boolean param9; assert param9 != null && param9
@Field Long paramCountTableRow; assert paramCountTableRow != null

configContent.testScript = 'complete test 1'
