package getl.lang

import groovy.transform.BaseScript
import groovy.transform.Field

@BaseScript Getl main

@Field Integer num = null

void check() {
    embeddedTable("test:table$num", true) {
        tableName = "table$num"
        field('field1')
    }
}

void done() {
    embeddedTable("test:table$num") {
        assert tableName == "table$num"
        assert fieldByName('field1') != null
        assert field.size() == 1
    }
}

embeddedTable("test:table$num") {
    tableName = "_table$num"
    field('field1').name = '_field1'
    field('_field2')
}
