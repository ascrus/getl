package getl.lang

import getl.tfs.TDS
import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript Getl main

@Field TDS con = null

assert scriptExtendedVars.password == '123'