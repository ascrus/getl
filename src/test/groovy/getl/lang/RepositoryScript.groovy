package getl.lang

import getl.h2.H2Table
import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript Getl main

forGroup 'test'

@Field H2Table test_table

assert test_table.tableName == 'table1'

assert files('#main').rootPath == '/tmp/main'
files('#child', true).rootPath = '/tmp/child'
assert files('#child').rootPath == '/tmp/child'

println 'main: ' + files('#main').rootPath
println 'child: ' + files('#child').rootPath