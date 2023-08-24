package getl.models

import getl.lang.Getl
import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript Getl main

@Field String step_name = null

println "Exec step $step_name ..."
unknownMethod()
