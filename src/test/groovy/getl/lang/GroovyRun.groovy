package getl.lang

import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript Getl main

@Field Integer param1 = configVars.param1
assert param1 > 0
println "param1=$param1"