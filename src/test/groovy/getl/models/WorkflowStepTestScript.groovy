package getl.models

import getl.lang.Getl
import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript Getl main

@Field String stepName; assert stepName != null
@Field Integer stepNum; assert stepNum != null
@Field Map<String, Object> map
@Field List<String> list

synchronized (configContent) {
    def countProcess = (configContent.countProcessed ?: 0) + 1
    configContent.countProcessed = countProcess
}

logInfo "Step \"$stepName\" from $stepNum complete (${configContent.countProcessed})"

return [processed: stepNum, map: map, list: list]