package getl.lang

import groovy.transform.BaseScript

@BaseScript Getl main

forGroup 'test'

assert files('#main').rootPath == '/tmp/main'
files('#child', true).rootPath = '/tmp/child'
assert files('#child').rootPath == '/tmp/child'

println 'main: ' + files('#main').rootPath
println 'child: ' + files('#child').rootPath