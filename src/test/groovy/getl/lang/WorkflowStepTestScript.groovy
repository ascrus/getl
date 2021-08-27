package getl.lang

import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript Getl main

@Field String stepName; assert stepName != null
@Field Integer stepNum; assert stepNum != null

logInfo "Step \"$stepName\" from $stepNum complete"

synchronized (configContent) {
    def countProcess = (configContent.countProcessed ?: 0)
    configContent.countProcessed = countProcess + 1
}