package getl.models

import getl.lang.Getl
import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript Getl main

@Field String stepName; assert stepName != null
@Field Integer stepNum; assert stepNum != null
@Field Map<String, Object> map = null
@Field List<String> list = null
@Field String macro = null
@Field Closure onEvent = null

void check() {
    def countProcess = (configContent.countProcessed ?: 0) + 1
    configContent.countProcessed = countProcess
    assert macro == "$stepNum:$stepName"
}

def varEvent = (onEvent != null)?onEvent():null
logInfo "Step \"$stepName\" from $stepNum complete (${configContent.countProcessed}) with macro \"$macro\"${(varEvent != null)?" and event \"$varEvent\"":''}"

return [processed: stepNum, map: map, list: list, ext_var1: scriptExtendedVars.ext_var1, varEvent: varEvent]
