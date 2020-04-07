package getl.test

import getl.lang.Getl
import groovy.transform.InheritConstructors

@InheritConstructors
class TestRunner extends Getl {
    @Override
    protected Class useInitClass() {
        TestInit
    }
}