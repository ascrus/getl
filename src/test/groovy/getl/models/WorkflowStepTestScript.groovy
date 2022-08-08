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
    assert macro == "$stepNum:$stepName", "Invalid [$macro] value from step [$stepNum]:[$stepName]"
}

def varEvent = onEvent?.call()
scriptEvents.get('EVENT')?.call(1)
logInfo "Step \"$stepName\" from $stepNum complete (${configContent.countProcessed}) with macro \"$macro\"${(varEvent != null)?" and event \"$varEvent\"":''}"

def varEvent1 = scriptEvents.eventFromAll('event1')?.call()
def varEvent2 = scriptEvents.eventFromObject('object1', 'event2')?.call()

return [processed: stepNum, map: map, list: list, ext_var1: scriptExtendedVars.ext_var1, varEvent: varEvent, varEvent1: varEvent1, varEvent2: varEvent2]
